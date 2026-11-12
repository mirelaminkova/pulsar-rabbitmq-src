"""Structure of the playbook, parameters and templates: secrets, ordering, removed relay code."""

import json
import re

import pytest

from tests.byoc_contract.validate import FIELDS as FORM_FIELDS
from tests.helpers import (
    FACTS,
    FORM_NOTES,
    PASSWORD,
    REPO,
    form_row,
    load_yaml,
    play,
    render,
    resolve,
    resolve_text,
    task_named,
    tasks,
)

# The relay is gone


@pytest.mark.parametrize(
    "path",
    [
        "relay_src",
        "Dockerfile",
        "compose.yml",
        ".dockerignore",
        "files/valkey.conf",
        "scripts",
        "templates/admin-location.conf.j2",
        "templates/admin-sync.service.j2",
        "templates/admin-sync.timer.j2",
    ],
)
def test_relay_valkey_and_admin_files_are_gone(path):
    assert not (REPO / path).exists()


@pytest.mark.parametrize(
    "path",
    [
        REPO / "playbook.yml",
        REPO / "parameters.yml",
        REPO / "group_vars" / "all.yml",
        *sorted((REPO / "tasks").glob("*.yml")),
        *sorted((REPO / "templates").iterdir()),
    ],
    ids=lambda path: path.name,
)
def test_no_broker_on_this_workspace(path):
    text = path.read_text().lower()
    found = [w for w in ("valkey", "rabbitmq-server", "docker_compose", "admin-sync") if w in text]
    assert found == []


# Parameters


def test_component_parameters():
    parameters = load_yaml("parameters.yml")

    assert "pulsar_galaxy_fqdn" not in parameters
    for name in (
        "pulsar_amqp_host",
        "pulsar_amqp_port",
        "pulsar_amqp_name",
        "pulsar_amqp_password",
        "pulsar_manager",
        "pulsar_storage_path",
        "pulsar_cvmfs",
        "pulsar_docker",
        "pulsar_apptainer",
        "pulsar_amqp_ack_republish_time",
    ):
        assert name in parameters
    assert parameters["pulsar_amqp_password"] == ""
    assert parameters["pulsar_amqp_host"] == "mq.galaxyproject.eu"
    assert parameters["pulsar_manager"] == "production"
    assert parameters["pulsar_amqp_ack_republish_time"] == "300"


def test_parameters_have_group_vars_defaults():
    group_vars = load_yaml("group_vars/all.yml")
    assert set(load_yaml("parameters.yml")) <= set(group_vars)


@pytest.mark.parametrize("name", ["_pulsar_amqp", "_pulsar_cvmfs"])
def test_parameter_defaults_agree_with_group_vars(name):
    # SRC passes parameters.yml's strings; without SRC, group_vars apply. Same result.
    group_vars = load_yaml("group_vars/all.yml")
    parameters = {
        key: value
        for key, value in load_yaml("parameters.yml").items()
        if key not in ("pulsar_amqp_name", "pulsar_amqp_password", "pulsar_storage_path")
    }
    defaults = {key: group_vars[key] for key in parameters}
    assert resolve(name, **parameters) == resolve(name, **defaults)


# Playbook


def test_restart_survives_a_later_failure():
    # A failing task after a changed app.yml must still restart Pulsar.
    assert play().get("force_handlers") is True


def test_validation_comes_first_and_never_shows_values():
    pre_tasks = play()["pre_tasks"]
    check = pre_tasks[0]["ansible.builtin.assert"]

    assert check["that"] == "_pulsar_amqp_errors | length == 0"
    assert "_pulsar_amqp_errors" in check["fail_msg"]
    assert pre_tasks[0].get("no_log") is not True  # the message must be visible


def secret_tasks():
    play_tasks = tasks(play()["tasks"])
    return [
        task
        for task in play_tasks + tasks("tasks/pulsar.yml")
        if "block" not in task
        and ("pulsar-app.yml.j2" in str(task) or "welcome.html.j2" in str(task))
    ]


def test_secret_tasks_are_found():
    # Guards the test below against matching nothing: app.yml and the welcome page.
    assert len(secret_tasks()) >= 2


@pytest.mark.parametrize("task", secret_tasks(), ids=lambda task: task["name"])
def test_tasks_with_secrets_are_not_logged(task):
    assert task.get("no_log") is True
    assert task.get("diff") is False


def test_app_yml_is_readable_by_pulsar_only():
    task = task_named(
        tasks("tasks/pulsar.yml"), "Write Pulsar's app.yml with the Galaxy broker connection"
    )
    module = task["ansible.builtin.template"]

    assert (module["owner"], module["group"], module["mode"]) == (
        "root",
        "{{ pulsar_user }}",
        "0640",
    )


@pytest.mark.parametrize(("docker", "groups"), [("true", ["docker"]), ("false", None)])
def test_pulsar_user_joins_docker_only_with_docker(docker, groups):
    # ansible-core 2.20's user module refuses append without groups (SRC runs 2.21).
    user = task_named(tasks("tasks/pulsar.yml"), "Create the Pulsar system user")[
        "ansible.builtin.user"
    ]
    omit = FACTS["omit"]
    arguments = {
        key: resolve_text(value, pulsar_docker=docker) if isinstance(value, str) else value
        for key, value in user.items()
    }
    arguments = {key: value for key, value in arguments.items() if value != omit}

    assert arguments.get("groups") == groups
    assert ("append" in arguments) is (groups is not None)
    assert arguments.get("append", True) in (True, "True", "true")


def test_pulsar_installed_with_amqp_support():
    task = task_named(
        tasks("tasks/pulsar.yml"), "Install pulsar-app with AMQP support into the venv"
    )

    assert "pulsar-app[amqp]=={{ pulsar_version }}" in task["ansible.builtin.pip"]["name"]
    assert resolve("pulsar_version") == "0.15.15"


def test_connection_check_after_restart_records_the_state():
    names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
    check = task_named(
        tasks("tasks/pulsar.yml"),
        "Check that Pulsar consumes its setup queue on Galaxy's broker",
    )
    outcome = task_named(tasks("tasks/pulsar.yml"), "Record the outcome of the broker check")
    state = task_named(tasks("tasks/pulsar.yml"), "Record the broker connection state")
    write = task_named(tasks("tasks/pulsar.yml"), "Write the broker connection state file")
    report = task_named(tasks("tasks/pulsar.yml"), "Fail when Pulsar cannot use Galaxy's broker")

    assert names.index("Apply pending Pulsar restarts before checking the connection") < (
        names.index(check["name"])
    )
    argv = check["ansible.builtin.command"]["argv"]
    assert argv[0] == "{{ pulsar_venv }}/bin/python"
    assert argv[1:] == ["{{ pulsar_check_script }}", "{{ pulsar_config_dir }}/app.yml"]
    assert check["until"] == "_pulsar_amqp_check.rc != 75"  # check_amqp.RETRY
    assert check["failed_when"] is False
    # The outcome's tasks are skipped with the check (CI's Slurm live check, check mode).
    assert outcome["tags"] == check["tags"] == ["broker-check"]
    assert outcome["when"] == "_pulsar_amqp_check.rc is defined"
    assert [t["name"] for t in outcome["block"]] == [
        state["name"],
        write["name"],
        "Show the broker connection state",
        report["name"],
    ]
    assert report["ansible.builtin.fail"]["msg"] == "{{ _pulsar_byoc_state.message }}"
    assert report["when"] == "_pulsar_byoc_state.state == 'error'"


def test_state_file_is_root_readable_by_pulsar_and_not_a_change():
    write = task_named(tasks("tasks/pulsar.yml"), "Write the broker connection state file")
    module = write["ansible.builtin.copy"]

    assert resolve_text(module["dest"]) == "/opt/pulsar/config/byoc-state.json"
    assert (module["owner"], resolve_text(module["group"]), module["mode"]) == (
        "root",
        "pulsar",
        "0640",
    )
    # checked_at changes with every run: the file reports the check, no configuration change.
    assert write["changed_when"] is False


def check_result(rc, stdout="", stderr=""):
    return {
        "rc": rc,
        "stdout": stdout,
        "stdout_lines": stdout.splitlines(),
        "stderr": stderr,
        "stderr_lines": stderr.splitlines(),
    }


@pytest.mark.parametrize(
    ("result", "state", "message"),
    [
        (
            check_result(0, "Pulsar consumes q in vhost v on h.\n"),
            "connected",
            "Pulsar consumes q in vhost v on h.",
        ),
        (
            check_result(3, "RabbitMQ on h refused the login of user u: ...\n"),
            "waiting",
            "RabbitMQ on h refused the login of user u: ...",
        ),
        (
            check_result(1, "Cannot resolve h: check pulsar_amqp_host.\n"),
            "error",
            "Cannot resolve h: check pulsar_amqp_host.",
        ),
        # After the retries: still exit 75 (unreachable broker).
        (
            check_result(75, "Cannot reach RabbitMQ on h (TimeoutError): ...\n"),
            "error",
            "Cannot reach RabbitMQ on h (TimeoutError): ...",
        ),
        # The check itself broke: Python's last line of the traceback.
        (
            check_result(
                1, "", "Traceback (most recent call last):\n  File x\nKeyError: 'managers'\n"
            ),
            "error",
            "KeyError: 'managers'",
        ),
        (check_result(127), "error", ""),
    ],
    ids=["connected", "waiting", "error", "retries-exhausted", "traceback", "no-output"],
)
def test_state_from_the_check_result(result, state, message):
    task = task_named(tasks("tasks/pulsar.yml"), "Record the broker connection state")
    fact = task["ansible.builtin.set_fact"]["_pulsar_byoc_state"]

    recorded = {key: resolve_text(value, _pulsar_amqp_check=result) for key, value in fact.items()}

    assert recorded["state"] == state
    assert recorded["message"] == message
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", recorded["checked_at"])
    assert PASSWORD not in str(recorded)


def test_state_file_content_is_the_state(tmp_path):
    write = task_named(tasks("tasks/pulsar.yml"), "Write the broker connection state file")
    state = {"state": "waiting", "message": "m", "checked_at": "2026-11-12T13:00:00Z"}

    content = resolve_text(write["ansible.builtin.copy"]["content"], _pulsar_byoc_state=state)

    assert json.loads(content) == state
    assert content.endswith("\n")


def test_state_is_unknown_until_the_check_ran():
    # The welcome page is templated with the fact; a skipped check leaves the default.
    assert resolve("_pulsar_byoc_state") == {"state": "unknown", "message": "", "checked_at": ""}


def test_welcome_page_is_templated_after_the_state_and_also_on_failure():
    play_tasks = play()["tasks"]
    [install] = [t for t in play_tasks if "always" in t]
    [include] = install["block"]
    [template] = [t for t in install["always"] if "ansible.builtin.template" in t]

    assert include["ansible.builtin.include_tasks"] == "tasks/pulsar.yml"
    assert template["ansible.builtin.template"]["src"] == "templates/welcome.html.j2"
    assert template["ansible.builtin.template"]["dest"] == "{{ pulsar_welcome_dir }}/index.html"
    assert template["when"] == "pulsar_configure_src_proxy | bool"
    assert (template.get("no_log"), template.get("diff")) == (True, False)
    # The directory and the proxy come first; the page only once, with the state.
    names = [t.get("name") for t in tasks(play_tasks)]
    assert names.index("Create the welcome page directory") < names.index(install["name"])
    assert sum("ansible.builtin.template" in t for t in tasks(play_tasks)) == 1
    # In tasks/pulsar.yml the state is set before the failure the page must survive.
    pulsar_names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
    assert pulsar_names.index("Record the broker connection state") < pulsar_names.index(
        "Fail when Pulsar cannot use Galaxy's broker"
    )


# Directories: the pulsar user (and so any job) must not be able to redirect root's
# directory task.

DIRECTORY_TASKS = ("Create root-owned Pulsar directories", "Create Pulsar's working directories")


def directory_paths(name):
    task = task_named(tasks("tasks/pulsar.yml"), name)
    loop = task["loop"]
    paths = resolve_text(loop) if isinstance(loop, str) else [resolve_text(p) for p in loop]
    owner = resolve_text(task["ansible.builtin.file"]["owner"])
    return task, owner, paths


@pytest.mark.parametrize("name", DIRECTORY_TASKS)
def test_links_are_not_followed(name):
    task, _, _ = directory_paths(name)
    assert task["ansible.builtin.file"]["follow"] is False


def test_pulsar_owns_only_leaves_in_root_owned_directories():
    _, root, root_owned = directory_paths(DIRECTORY_TASKS[0])
    _, owner, pulsar_owned = directory_paths(DIRECTORY_TASKS[1])

    storage = task_named(tasks("tasks/storage.yml"), "Create the root-owned storage directory")
    root_owned.append(resolve_text(storage["ansible.builtin.file"]["path"]))

    assert (root, owner) == ("root", "pulsar")
    assert pulsar_owned
    for path in pulsar_owned:
        assert path.rsplit("/", 1)[0] in root_owned, path


def test_unit_is_installed_before_tasks_that_can_fail():
    # force_handlers restarts Pulsar after a failure; the unit must exist by then.
    names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
    unit = names.index("Install the Pulsar systemd unit")
    assert unit < names.index("Install Apptainer for Singularity-enabled Galaxy environments")
    assert unit < names.index("Check that the Galaxy CVMFS repositories mount")


# Templates: the welcome page (templates/welcome.html.j2)

# FORM_FIELDS: byoc-sync's byoc_pulsar form, in its order (vendored, checked against its main).
# The state's tag and the text next to it; for error, the check's message (MESSAGE).
MESSAGE = "the broker refused the connection"
STATE_TAGS = {
    "connected": ("Connected", "Galaxy's broker accepts this Pulsar"),
    "waiting": ("Waiting", "waiting for the Galaxy admins to create your account"),
    "error": ("Error", MESSAGE),
    "unknown": ("Not checked yet", "the connection check has not run yet"),
}


def byoc_state(state, message="", checked_at=""):
    return {"_pulsar_byoc_state": {"state": state, "message": message, "checked_at": checked_at}}


def test_welcome_page_shows_the_byoc_form_in_the_forms_order():
    # What the user enters in Galaxy's preferences (byoc_pulsar): the form byoc-sync reads.
    page = render("welcome.html.j2")
    values = {**resolve("_pulsar_byoc_form"), "password": PASSWORD}

    assert "byoc_pulsar" in page
    assert "Enter this in Galaxy" in page
    assert set(values) == set(FORM_FIELDS)
    positions = []
    for field in FORM_FIELDS:
        row = form_row(field, values[field])
        assert row in page, field
        positions.append(page.index(row))
    assert positions == sorted(positions)
    for field in FORM_NOTES:  # the note is outside the copied value
        assert form_row(field, values[field], note="") not in page
    assert "/data/share" in page
    assert page.count(PASSWORD) == 1
    assert "User &rsaquo; Preferences &rsaquo; Manage information" in page
    assert "reload this page" in page


def test_welcome_page_shows_the_broker_version_and_storage():
    page = render("welcome.html.j2")

    assert "<dd><code>amqps://mq.example.org:5671</code></dd>" in page
    assert (
        "<dd><code>galaxy_hpc_byoc</code>, vhost <code>/pulsar/galaxy_hpc_byoc</code></dd>" in page
    )
    assert "<dd>0.15.15 on <code>pulsar.example.org</code></dd>" in page
    assert (
        "<dd><code>/data/pulsar_storage/pulsar</code>, staging "
        "<code>/data/pulsar_storage/pulsar/staging</code></dd>" in page
    )


def test_welcome_page_follows_the_resolved_credentials():
    # tasks/credentials.yml replaces _pulsar_amqp (set_fact) with the generated credentials.
    params = resolve("_pulsar_amqp_params")
    amqp = resolve("_pulsar_amqp", _pulsar_amqp_params={**params, "name": "srcdeadbeef"})

    page = render("welcome.html.j2", _pulsar_amqp=amqp)

    assert "<code>galaxy_srcdeadbeef_byoc</code>" in page
    assert form_row("username", "srcdeadbeef") in page
    assert "<code>hpc</code>" not in page
    assert "galaxy_hpc_byoc" not in page


def test_welcome_page_shows_the_default_broker():
    # pulsar_amqp_host unset (an empty string from SRC): usegalaxy.eu's broker.
    page = render("welcome.html.j2", pulsar_amqp_host="")

    assert "<code>amqps://mq.galaxyproject.eu:5671</code>" in page
    assert "mq.example.org" not in page


@pytest.mark.parametrize(("state", "expected"), STATE_TAGS.items())
def test_welcome_page_shows_the_connection_state(state, expected):
    page = render("welcome.html.j2", **byoc_state(state, message=MESSAGE))
    tag, text = expected

    assert f'<span class="state state-{state}">{tag}</span>' in page
    assert text in page
    assert (MESSAGE in page) == (state == "error")
    for other in set(STATE_TAGS) - {state}:
        assert f'class="state state-{other}"' not in page


def test_welcome_page_shows_an_error_without_a_message():
    page = render("welcome.html.j2", **byoc_state("error"))
    assert '<span class="state state-error">Error</span>' in page
    assert "the connection check failed" in page


@pytest.mark.parametrize(
    "params",
    [byoc_state("mystery"), byoc_state("Connected"), {"_pulsar_byoc_state": {}}]
    + [{"_pulsar_byoc_state": value} for value in (None, "", '{"state": "connected"}', [])],
    ids=repr,
)
def test_welcome_page_treats_an_unexpected_or_malformed_state_as_unknown(params):
    # A state file from an earlier version must never break the deployment.
    page = render("welcome.html.j2", **params)
    assert '<span class="state state-unknown">Not checked yet</span>' in page
    assert "state-connected" not in page[page.index("<body>") :]


@pytest.mark.parametrize("state", list(STATE_TAGS))
def test_welcome_page_leaves_the_check_time_off(state):
    # checked_at changes every run; a page with it would make the template task change on
    # every rerun (molecule's idempotence step, the Slurm live rerun check).
    page = render("welcome.html.j2", **byoc_state(state, checked_at="2026-11-12T10:00:00Z"))
    assert "2026-11-12" not in page
    assert "checked at" not in page


def test_welcome_page_shows_not_checked_yet_by_default():
    # group_vars' default: the tolerant check (issue #22) overrides it when it has run.
    assert resolve("_pulsar_byoc_state") == {"state": "unknown", "message": "", "checked_at": ""}
    assert '<span class="state state-unknown">Not checked yet</span>' in render("welcome.html.j2")


def test_welcome_page_shows_the_troubleshooting_commands():
    page = render("welcome.html.j2")

    assert "systemctl status pulsar" in page
    assert "journalctl -u pulsar -f" in page
    assert (
        "sudo /opt/pulsar/venv/bin/python /opt/pulsar/check_amqp.py /opt/pulsar/config/app.yml"
        in page
    )


@pytest.mark.parametrize(
    "text",
    [
        "Deployment settings",
        "galaxy-amqp-tools",
        "galaxy-amqp-param",
        "src_galaxy_pulsar_amqp_configs",
        "Important Notes",
        'class="flow"',
        "How Galaxy and Pulsar connect",
        "Connect Galaxy",
        "galaxy_fqdn",
        "galaxy_entry",
        "copy-box",
        "checked that",
    ],
)
def test_welcome_page_has_no_text_of_earlier_pages(text):
    assert text not in render("welcome.html.j2")


@pytest.mark.parametrize("runner", ["slurm", "local"])
@pytest.mark.parametrize("docker", ["true", "false"])
@pytest.mark.parametrize("apptainer", ["true", "false"])
@pytest.mark.parametrize("cvmfs", ["true", "false"])
def test_welcome_page_renders_for_every_combination(runner, docker, apptainer, cvmfs):
    page = render(
        "welcome.html.j2",
        pulsar_job_runner=runner,
        pulsar_docker=docker,
        pulsar_apptainer=apptainer,
        pulsar_cvmfs=cvmfs,
    )

    # trim_blocks (ansible.builtin.template's setting) eats the {% set %} lines.
    assert page.startswith("<!doctype html>")
    assert page.rstrip().endswith("</html>")
    assert ("<code>queued_drmaa</code>" in page) == (runner == "slurm")
    assert ("4 CPUs, 14400 MB" in page) == (runner == "slurm")
    assert ("<code>queued_python</code>" in page) == (runner == "local")
    assert ("3 concurrent jobs" in page) == (runner == "local")
    assert ("squeue" in page) == (runner == "slurm")
    assert ('<li class="tag">Docker</li>' in page) == (docker == "true")
    assert ('<li class="tag">Apptainer</li>' in page) == (apptainer == "true")
    assert ('<li class="tag">CVMFS</li>' in page) == (cvmfs == "true")
    assert ("<dd>none</dd>" in page) == ((docker, apptainer, cvmfs) == ("false", "false", "false"))
    assert page.count(PASSWORD) == 1


def test_welcome_page_keeps_the_layout_notice_and_noindex():
    page = render("welcome.html.j2")
    assert '<meta name="robots" content="noindex">' in page
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page
    assert "concrete.min.css" in page
    assert 'alt="Pulsar logo"' in page
    assert 'alt="SURF logo"' in page
    assert "RabbitMQ password" in page
    assert "members" in page
    assert page.index("members") < page.index("Enter this in Galaxy") < page.index("Status")
    assert page.index("Status") < page.index("Troubleshooting")


XSS = "<script>alert('x')</script>"
ESCAPED_XSS = "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"


@pytest.mark.parametrize(
    "params",
    [
        {"workspace_fqdn": XSS},
        {"pulsar_manager": XSS},
        {"pulsar_manager": XSS, "pulsar_job_runner": "local"},
        {"pulsar_amqp_name": XSS},
        {"pulsar_amqp_password": XSS},
        {"pulsar_amqp_host": XSS},
        {"pulsar_amqp_port": XSS},
        {"pulsar_version": XSS},
        {"pulsar_storage_path": f"/data/{XSS}"},
        {"pulsar_venv": XSS},
        {"pulsar_check_script": XSS},
        {"pulsar_config_dir": XSS},
        {"pulsar_num_concurrent_jobs": XSS, "pulsar_job_runner": "local"},
        {"_pulsar_slurm_cpus": XSS},
        {"_pulsar_slurm_memory_mb": XSS},
        byoc_state("error", message=XSS),
        {"_pulsar_byoc_form": dict.fromkeys(set(FORM_FIELDS) - {"password"}, XSS)},
    ],
    ids=lambda params: "+".join(params),
)
def test_welcome_page_escapes_every_value(params):
    # Most values are validated before the page is written; the template escapes anyway.
    page = render("welcome.html.j2", **params)
    body = page[page.index("<body>") :]
    assert XSS not in page
    assert ESCAPED_XSS in body
    assert "<script>" not in body.split("</main>")[0]


def test_service_runs_pulsar_main_as_pulsar():
    unit = render("pulsar.service.j2")

    assert "User=pulsar" in unit
    # files/pulsar_main.py runs pulsar-main and exits when a broker consumer dies.
    assert re.search(
        r"^ExecStart=/opt/pulsar/venv/bin/python -I /opt/pulsar/pulsar_main.py "
        r"--config_dir /opt/pulsar/config --ini_path /opt/pulsar/config/server.ini$",
        unit,
        re.M,
    )


def unit_section(unit, name):
    section = unit.split(f"[{name}]\n", 1)[1].split("\n[", 1)[0]
    return [line for line in section.splitlines() if line and not line.startswith("#")]


def test_service_keeps_retrying_a_refused_login():
    # A refused login (no BYOC account yet) exits Pulsar: restarted every 30 s, forever.
    unit = render("pulsar.service.j2")

    assert "StartLimitIntervalSec=0" in unit_section(unit, "Unit")
    service = unit_section(unit, "Service")
    assert "Restart=always" in service
    assert "RestartSec=30" in service


def test_launcher_is_installed_before_the_unit_and_restarts_pulsar():
    names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
    launcher = task_named(tasks("tasks/pulsar.yml"), "Install the Pulsar launcher")
    module = launcher["ansible.builtin.copy"]

    assert names.index(launcher["name"]) < names.index("Install the Pulsar systemd unit")
    assert module["src"] == "files/pulsar_main.py"
    assert resolve_text(module["dest"]) == "/opt/pulsar/pulsar_main.py"
    assert (module["owner"], module["group"], module["mode"]) == ("root", "root", "0755")
    assert launcher["notify"] == "Restart Pulsar"
