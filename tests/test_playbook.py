"""Structure of the playbook, parameters and templates: secrets, ordering, removed relay code."""

import html
import re

import pytest

from tests.galaxy_contract import src_galaxy
from tests.helpers import (
    FACTS,
    PASSWORD,
    REPO,
    facts,
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


def test_component_parameters_map_onto_galaxy():
    parameters = load_yaml("parameters.yml")

    for name in (
        "pulsar_galaxy_fqdn",
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
        if key
        not in (
            "pulsar_galaxy_fqdn",
            "pulsar_amqp_name",
            "pulsar_amqp_password",
            "pulsar_storage_path",
        )
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


def test_connection_check_after_restart_and_fails_clearly():
    names = [t.get("name") for t in tasks("tasks/pulsar.yml")]
    check = task_named(
        tasks("tasks/pulsar.yml"),
        "Check that Pulsar consumes its setup queue on Galaxy's broker",
    )
    report = task_named(tasks("tasks/pulsar.yml"), "Fail when Pulsar cannot use Galaxy's broker")

    assert names.index("Apply pending Pulsar restarts before checking the connection") < (
        names.index(check["name"])
    )
    assert names.index(report["name"]) == names.index(check["name"]) + 1
    argv = check["ansible.builtin.command"]["argv"]
    assert argv[0] == "{{ pulsar_venv }}/bin/python"
    assert argv[1:] == ["{{ pulsar_check_script }}", "{{ pulsar_config_dir }}/app.yml"]
    assert check["until"] == "_pulsar_amqp_check.rc != 75"  # check_amqp.RETRY
    assert "_pulsar_amqp_check.stdout" in report["ansible.builtin.fail"]["msg"]
    assert report["when"] == "_pulsar_amqp_check.rc != 0"


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


# Templates


def test_welcome_page_shows_the_galaxy_entry():
    page = render("welcome.html.j2")
    entry = resolve("_pulsar_amqp")["galaxy_entry"]

    assert f'<pre id="galaxy-amqp-param">{html.escape(entry)}</pre>' in page
    assert "src_galaxy_pulsar_amqp_configs" in page
    tools = re.search(r'<pre id="galaxy-amqp-tools">([^<]*)</pre>', page).group(1)
    assert {m["environment"] for m in src_galaxy.src_config_list(tools, "tool mappings")} == {
        "amqp_hpc"
    }
    assert "src_galaxy_rabbitmq" in page
    # Galaxy's preferences list the entry by its label, which defaults to the name.
    assert "choosing <code>hpc</code>" in page


def test_welcome_page_shows_the_galaxy_host_as_the_broker():
    page = render("welcome.html.j2")

    assert "amqps://galaxy.example.org:5671" in page
    assert "Galaxy runs RabbitMQ" in page
    assert "pulsar_amqp_host" not in page


def test_welcome_page_shows_a_separate_broker_host():
    page = render(
        "welcome.html.j2", pulsar_galaxy_fqdn="usegalaxy.eu", pulsar_amqp_host="mq.galaxyproject.eu"
    )

    assert "amqps://mq.galaxyproject.eu:5671" in page
    assert "amqps://usegalaxy.eu" not in page
    assert "RabbitMQ on mq.galaxyproject.eu:5671" in page
    assert "Galaxy runs RabbitMQ" not in page
    assert "skip this section" in page
    # Galaxy's own name still identifies it and its entry.
    assert "<code>usegalaxy.eu</code> must have this Pulsar" in page


def test_welcome_page_does_not_claim_a_check_that_may_have_failed():
    # The page is published before the broker check, so it must not report its result.
    assert "checked that" not in render("welcome.html.j2")


def test_welcome_page_escapes_values():
    page = render("welcome.html.j2", **facts(hostname="<b>x</b>"))
    assert "<b>x</b>" not in page


def test_welcome_page_contains_the_password_only_in_the_entry():
    page = render("welcome.html.j2")
    assert page.count(PASSWORD) == 1


def test_service_runs_pulsar_main_as_pulsar():
    unit = render("pulsar.service.j2")

    assert "User=pulsar" in unit
    assert re.search(r"^ExecStart=/opt/pulsar/venv/bin/pulsar-main ", unit, re.M)
