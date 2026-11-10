"""Pulsar's data on SRC attached storage: the required pulsar_storage_path.

Covers the parameter's validation and what is derived from it (Galaxy's jobs_directory,
the directory layout, Docker's data-root), and the tasks and templates that use it. The
volume check itself, files/check_storage.py, is tested in tests/test_check_storage.py.
"""

import os
import subprocess

import pytest

from filter_plugins.pulsar_amqp import pulsar_amqp_errors
from tests.galaxy_contract import src_galaxy
from tests.helpers import (
    PARAMS,
    condition,
    load_yaml,
    render,
    render_yaml,
    resolve,
    resolve_text,
    settings,
    task_named,
    tasks,
)

STORAGE = PARAMS["pulsar_storage_path"]


def storage_errors(value):
    return [e for e in pulsar_amqp_errors(settings(storage_path=value)) if "storage" in e]


# --- Parameter -----------------------------------------------------------------


@pytest.mark.parametrize("value", ["", "  ", None])
def test_storage_path_is_required(value):
    [error] = storage_errors(value)
    assert "pulsar_storage_path is required" in error
    assert "attach a storage volume" in error


@pytest.mark.parametrize(
    "value",
    [
        "/",
        "/data",
        "/data/pulsar_storage",  # the volume itself, not a directory on it
        "/mnt/pulsar/files",  # not where SRC mounts attached volumes
        "/var/lib/pulsar/x",
        "/opt/pulsar/config",
        "/data/../etc/pulsar",
        "/data/vol/../../etc",
        "/data/./vol/pulsar",
        "/data/vol/pulsar/",
        "/data/vol//pulsar",
        "/data/vol/pul sar",
        "/data/vol/a,b",
        "/data/vol/{a}",
        "data/vol/pulsar",
    ],
)
def test_storage_path_must_be_a_directory_on_a_data_volume(value):
    [error] = storage_errors(value)
    assert "/data/<volume name>/" in error
    assert value in error


@pytest.mark.parametrize("value", ["/data/pulsar_storage/pulsar", "/data/vol-2/a/b.c_d"])
def test_valid_storage_paths(value):
    assert pulsar_amqp_errors(settings(storage_path=value)) == []


def test_storage_path_is_trimmed():
    assert settings(storage_path=f" {STORAGE} ")["storage_path"] == STORAGE


def test_staging_directory_is_derived_from_the_storage_path():
    assert settings(storage_path="/data/vol/pulsar")["jobs_directory"] == "/data/vol/pulsar/staging"


def test_there_is_no_separate_staging_parameter():
    assert "pulsar_staging_dir" not in load_yaml("parameters.yml")
    assert "pulsar_staging_dir" not in load_yaml("group_vars/all.yml")


def test_storage_parameter_has_no_default():
    assert load_yaml("parameters.yml")["pulsar_storage_path"] == ""
    errors = resolve("_pulsar_amqp_errors", pulsar_storage_path="")
    assert any("pulsar_storage_path is required" in error for error in errors)


# --- Galaxy entry ----------------------------------------------------------------


def parsed_entry(galaxy_entry):
    entries = src_galaxy.src_config_list(galaxy_entry, "src_galaxy_pulsar_amqp_configs")
    assert src_galaxy.pulsar_amqp_config_errors(entries) == []
    [config] = src_galaxy.pulsar_amqp_configs(entries)
    return config


@pytest.mark.parametrize("storage", [STORAGE, "/data/volume_2/a.b/pulsar-1"])
def test_galaxy_parses_the_derived_jobs_directory(storage):
    result = resolve("_pulsar_amqp", pulsar_storage_path=storage)
    assert parsed_entry(result["galaxy_entry"])["jobs_directory"] == f"{storage}/staging"


def test_welcome_page_entry_matches_app_yml_staging_directory():
    page = render("welcome.html.j2", pulsar_storage_path="/data/vol/pulsar")
    app = render_yaml("pulsar-app.yml.j2", pulsar_storage_path="/data/vol/pulsar")
    entry = resolve("_pulsar_amqp", pulsar_storage_path="/data/vol/pulsar")["galaxy_entry"]

    assert "jobs_directory: /data/vol/pulsar/staging" in page
    assert parsed_entry(entry)["jobs_directory"] == app["staging_directory"]


def test_welcome_page_shows_the_storage_and_its_directories():
    page = render("welcome.html.j2", pulsar_storage_path="/data/vol/pulsar", pulsar_docker="true")
    assert "<strong>Storage:</strong> <code>/data/vol/pulsar</code>" in page
    for leaf in ("persisted_data", "apptainer_cache", "apptainer_tmp", "docker"):
        assert f"<code>/data/vol/pulsar/{leaf}</code>" in page
    page = render("welcome.html.j2", pulsar_apptainer="false")
    assert "apptainer_cache" not in page
    assert f"{STORAGE}/docker" not in page


# --- Layout ----------------------------------------------------------------------

LAYOUT = {
    "_pulsar_amqp.jobs_directory": f"{STORAGE}/staging",
    "pulsar_persistence_dir": f"{STORAGE}/persisted_data",
    "pulsar_apptainer_cache_dir": f"{STORAGE}/apptainer_cache",
    "pulsar_apptainer_tmp_dir": f"{STORAGE}/apptainer_tmp",
    "pulsar_docker_data_root": f"{STORAGE}/docker",
    "pulsar_job_tmp_dir": f"{STORAGE}/job_tmp",
    "pulsar_home": f"{STORAGE}/home",
    "pulsar_slurmd_spool_dir": f"{STORAGE}/slurmd",
}


@pytest.mark.parametrize(("name", "path"), LAYOUT.items())
def test_data_lives_on_the_storage(name, path):
    assert resolve(name) == path


def test_installation_and_configuration_stay_off_the_storage():
    for name in ("pulsar_venv", "pulsar_config_dir", "pulsar_storage_check_script"):
        assert not resolve(name).startswith(STORAGE)


def test_app_yml_uses_the_storage():
    app = render_yaml("pulsar-app.yml.j2")
    assert app["staging_directory"] == f"{STORAGE}/staging"
    assert app["persistence_directory"] == f"{STORAGE}/persisted_data"


def test_docker_keeps_images_and_containers_on_the_storage():
    # Without the containerd image store, which keeps images in /var/lib/containerd.
    assert resolve("docker_daemon_options") == {
        "data-root": f"{STORAGE}/docker",
        "features": {"containerd-snapshotter": False},
    }


def test_service_checks_the_storage_and_points_apptainer_at_it():
    unit = render("pulsar.service.j2")
    # A failed or late mount would fail the start as a dependency, never retried.
    assert "RequiresMountsFor" not in unit
    assert f"WorkingDirectory={STORAGE}/home\n" in unit
    check = "/usr/local/libexec/pulsar-src/check_storage.py"
    assert f"ExecStartPre=+/usr/bin/python3 -I {check} {STORAGE}\n" in unit
    assert f"Environment=APPTAINER_CACHEDIR={STORAGE}/apptainer_cache\n" in unit
    assert f"Environment=APPTAINER_TMPDIR={STORAGE}/apptainer_tmp\n" in unit


def test_service_without_apptainer_sets_no_apptainer_directories():
    unit = render("pulsar.service.j2", pulsar_apptainer="false")
    assert "APPTAINER" not in unit


# --- Tasks -----------------------------------------------------------------------

STORAGE_TASKS = tasks("tasks/storage.yml")
PULSAR_TASKS = tasks("tasks/pulsar.yml")


def test_storage_is_checked_before_anything_is_installed():
    [play] = load_yaml("playbook.yml")
    names = [task["name"] for task in play["pre_tasks"]]
    storage = names.index("Check and prepare the attached storage")

    assert names.index("Validate the component parameters") < storage
    # pre_tasks run before the roles (Docker, CVMFS) and tasks.
    assert play["pre_tasks"][storage]["ansible.builtin.include_tasks"] == "tasks/storage.yml"
    first = STORAGE_TASKS[0]
    assert first["name"] == "Check that the storage path is on an attached volume"
    assert first["args"]["executable"] == "/usr/bin/python3"
    assert first["check_mode"] is False


@pytest.mark.parametrize(
    ("docker", "check_mode", "expected"),
    [
        ("false", False, "--protect "),
        ("true", False, "--protect --docker "),
        ("true", True, "--docker "),  # check mode changes nothing
    ],
)
def test_storage_check_gets_the_path_and_docker(docker, check_mode, expected):
    command = resolve_text(
        STORAGE_TASKS[0]["ansible.builtin.script"],
        pulsar_docker=docker,
        ansible_check_mode=check_mode,
    )
    assert command.split() == f"files/check_storage.py {expected}{STORAGE}".split()


def test_storage_check_reports_a_protected_volume_root_as_a_change():
    task = STORAGE_TASKS[0]
    for stdout, changed in (("ok\n", False), ("set the sticky bit on /data/vol, so\nok\n", True)):
        assert condition(task["changed_when"], _pulsar_storage_check={"stdout": stdout}) is changed


def test_the_replaced_staging_parameter_is_refused():
    [play] = load_yaml("playbook.yml")
    task = task_named(play["pre_tasks"], "Refuse the replaced staging directory parameter")
    check = task["ansible.builtin.assert"]
    names = [t["name"] for t in play["pre_tasks"]]
    assert names.index(task["name"]) < names.index("Check supported workspace OS")
    for value, ok in ((None, True), ("", True), ("/mnt/pulsar/files/staging", False)):
        variables = {} if value is None else {"pulsar_staging_dir": value}
        assert condition(check["that"], **variables) is ok
    assert "pulsar_storage_path" in check["fail_msg"]


@pytest.mark.parametrize(
    ("result", "fails", "message"),
    [
        ({"rc": 0, "stdout": "ok\n", "stderr": ""}, False, None),
        ({"rc": 1, "stdout": "on the root fs\n", "stderr": ""}, True, "on the root fs"),
        ({"failed": True, "msg": "python3 not found"}, True, "python3 not found"),
    ],
)
def test_storage_check_failure_is_reported(result, fails, message):
    report = task_named(STORAGE_TASKS, "Fail when the storage path is unusable")
    assert condition(report["when"], _pulsar_storage_check=result) is fails
    if message:
        text = resolve_text(report["ansible.builtin.fail"]["msg"], _pulsar_storage_check=result)
        assert text == message


def test_storage_root_is_root_owned_and_links_are_not_followed():
    task = task_named(STORAGE_TASKS, "Create the root-owned storage directory")
    module = task["ansible.builtin.file"]

    assert resolve_text(module["path"]) == STORAGE
    assert (module["owner"], module["group"], module["mode"]) == ("root", "root", "0755")
    assert module["follow"] is False


def test_check_script_is_installed_for_the_units():
    task = task_named(
        STORAGE_TASKS, "Install the storage check for the start of Pulsar, Docker and slurmd"
    )
    module = task["ansible.builtin.copy"]
    assert module["src"] == "files/check_storage.py"
    assert resolve_text(module["dest"]) == "/usr/local/libexec/pulsar-src/check_storage.py"
    assert (module["owner"], module["mode"]) == ("root", "0755")
    directory = task_named(STORAGE_TASKS, "Create the root-only directory of the storage check")
    directory = directory["ansible.builtin.file"]
    assert resolve_text(directory["path"]) == "/usr/local/libexec/pulsar-src"
    assert (directory["owner"], directory["mode"], directory["follow"]) == ("root", "0755", False)


def owned_dirs(**params):
    task = task_named(PULSAR_TASKS, "Create Pulsar's working directories")
    return resolve_text(task["loop"], **params)


def test_apptainer_directories_only_with_apptainer():
    assert {f"{STORAGE}/apptainer_cache", f"{STORAGE}/apptainer_tmp"} <= set(owned_dirs())
    assert not [leaf for leaf in owned_dirs(pulsar_apptainer="false") if "apptainer" in leaf]


HANDOVER = (
    "Give reused Pulsar data of another uid to the pulsar user",
    "Give reused Pulsar data of another gid to the pulsar group",
)
STOP = "Stop Pulsar before handing over reused data"
USER = {"uid": 990, "group": 990}
LINK = {"exists": True, "isdir": False, "islnk": True, "uid": 1, "gid": 1}


def found(uid, gid):
    return {"exists": True, "isdir": True, "uid": uid, "gid": gid}


@pytest.mark.parametrize(
    ("stat", "stop", "chown"),
    [
        ({"exists": False}, False, (False, False)),
        (found(990, 990), False, (False, False)),
        (found(991, 990), True, (True, False)),
        (found(990, 991), True, (False, True)),
        (found(991, 991), True, (True, True)),
        (found(0, 0), False, (False, False)),  # root's files stay root's
        (found(0, 991), True, (False, True)),
        (LINK, False, (False, False)),
    ],
)
def test_reused_data_of_another_owner_is_handed_over(stat, stop, chown):
    item = {"item": f"{STORAGE}/staging", "stat": stat}
    others = [{"item": f"{STORAGE}/persisted_data", "stat": found(990, 990)}]
    variables = {"_pulsar_reused": {"results": [item, *others]}, "_pulsar_user": USER}
    task = task_named(PULSAR_TASKS, STOP)
    assert condition(task["when"], **variables) is stop
    for name, runs in zip(HANDOVER, chown, strict=True):
        task = task_named(PULSAR_TASKS, name)
        assert condition(task["when"], item=item, **variables) is runs, name


def test_handover_conditions_survive_check_mode_without_a_uid():
    item = {"item": f"{STORAGE}/staging", "stat": found(991, 991)}
    variables = {"_pulsar_reused": {"results": [item]}, "_pulsar_user": {}}
    assert condition(task_named(PULSAR_TASKS, STOP)["when"], **variables) is True
    for name in HANDOVER:
        assert condition(task_named(PULSAR_TASKS, name)["when"], item=item, **variables)


def test_handover_changes_only_the_old_owners_files_without_following_links():
    item = {"item": f"{STORAGE}/staging", "stat": {"uid": 991, "gid": 992}}
    uid, gid = (task_named(PULSAR_TASKS, name)["ansible.builtin.command"] for name in HANDOVER)
    common = ["chown", "--recursive", "--no-dereference"]

    argv = [resolve_text(arg, item=item) for arg in uid["argv"]]
    assert argv == [*common, "--from=991", "pulsar:pulsar", item["item"]]
    argv = [resolve_text(arg, item=item) for arg in gid["argv"]]
    assert argv == [*common, "--from=:992", ":pulsar", item["item"]]


def test_handover_runs_with_pulsar_stopped_and_before_the_directories_are_created():
    names = [task["name"] for task in PULSAR_TASKS]
    steps = ("Find Pulsar data from an earlier workspace", STOP, *HANDOVER)
    order = [names.index(name) for name in steps]
    assert order == sorted(order)
    assert order[-1] < names.index("Create Pulsar's working directories")
    stop = task_named(PULSAR_TASKS, STOP)
    # Started again even when a later task fails.
    assert stop["notify"] == "Restart Pulsar"
    assert stop["ansible.builtin.command"]["argv"] == ["systemctl", "stop", "pulsar.service"]
    # 5: the unit does not exist yet.
    for rc, failed in ((0, False), (5, False), (1, True)):
        assert condition(stop["failed_when"], _pulsar_stop={"rc": rc}) is failed


# --- Slurm on the storage --------------------------------------------------------

TASK_PROLOG = "/etc/slurm/task_prolog.sh"
TASK_EPILOG = "/etc/slurm/task_epilog.sh"


def test_slurm_keeps_job_files_on_the_storage_and_its_controller_state_local():
    config = resolve("slurm_config")
    assert config["SlurmdSpoolDir"] == f"{STORAGE}/slurmd"
    assert config["TmpFS"] == f"{STORAGE}/job_tmp"
    assert config["TaskProlog"] == TASK_PROLOG
    assert config["TaskEpilog"] == TASK_EPILOG
    assert "StateSaveLocation" not in config  # the role's /var/lib/slurm/slurmctld


def run_script(template, tmp_path, job_id, **params):
    """Render a Slurm task script with tmp_path as the storage and run it like slurmd."""
    script = tmp_path / template
    script.write_text(render(template, pulsar_storage_path=str(tmp_path), **params))
    environment = {"PATH": os.environ["PATH"]}
    if job_id is not None:
        environment["SLURM_JOB_ID"] = job_id
    return subprocess.run(  # noqa: S603 - the rendered test script
        ["/bin/sh", str(script)], env=environment, capture_output=True, text=True, check=True
    ).stdout


@pytest.mark.parametrize("apptainer", ["true", "false"])
def test_task_prolog_gives_each_slurm_job_its_storage_directories(tmp_path, apptainer):
    (tmp_path / "job_tmp").mkdir()
    lines = run_script("task_prolog.sh.j2", tmp_path, "42", pulsar_apptainer=apptainer)
    lines = lines.splitlines()

    assert lines[0] == f"export TMPDIR={tmp_path}/job_tmp/42"
    assert (tmp_path / "job_tmp" / "42").is_dir()
    apptainer_lines = [
        f"export APPTAINER_CACHEDIR={tmp_path}/apptainer_cache",
        f"export APPTAINER_TMPDIR={tmp_path}/apptainer_tmp",
    ]
    assert lines[1:] == (apptainer_lines if apptainer == "true" else [])


def test_task_epilog_removes_the_jobs_temporary_directory(tmp_path):
    (tmp_path / "job_tmp" / "42" / "sub").mkdir(parents=True)
    (tmp_path / "job_tmp" / "43").mkdir()
    run_script("task_epilog.sh.j2", tmp_path, "42")
    assert os.listdir(tmp_path / "job_tmp") == ["43"]


@pytest.mark.parametrize("job_id", [None, "", "4 2", "../x", "*"])
@pytest.mark.parametrize("template", ["task_prolog.sh.j2", "task_epilog.sh.j2"])
def test_task_scripts_touch_nothing_without_a_numeric_job_id(tmp_path, template, job_id):
    (tmp_path / "job_tmp").mkdir()
    (tmp_path / "x").mkdir()
    assert run_script(template, tmp_path, job_id) == ""
    assert set(os.listdir(tmp_path)) == {"job_tmp", "x", template}
    assert os.listdir(tmp_path / "job_tmp") == []


def test_task_prolog_is_installed_for_slurm():
    task = task_named(
        tasks("tasks/slurm.yml"), "Install the task prolog that keeps job files on the storage"
    )
    module = task["ansible.builtin.template"]
    assert module["src"] == "templates/task_prolog.sh.j2"
    assert resolve_text(module["dest"]) == TASK_PROLOG
    assert (module["owner"], module["mode"]) == ("root", "0755")


def test_local_jobs_get_the_storage_tmp_from_the_unit():
    for runner in ("slurm", "local"):
        unit = render("pulsar.service.j2", pulsar_job_runner=runner)
        assert f"Environment=TMPDIR={STORAGE}/job_tmp\n" in unit


def test_job_tmp_belongs_to_pulsar():
    assert f"{STORAGE}/job_tmp" in owned_dirs()


def test_slurmd_spool_is_roots_on_the_storage_with_slurm_only():
    task = task_named(STORAGE_TASKS, "Create slurmd's spool directory on the storage")
    module = task["ansible.builtin.file"]
    assert resolve_text(module["path"]) == f"{STORAGE}/slurmd"
    assert (module["owner"], module["mode"], module["follow"]) == ("root", "0755", False)
    assert condition(task["when"], pulsar_job_runner="slurm") is True
    assert condition(task["when"], pulsar_job_runner="local") is False


@pytest.mark.parametrize(
    ("unit", "option"), [("docker", "--docker "), ("slurmd", "")], ids=["docker", "slurmd"]
)
def test_units_on_the_storage_start_only_with_it(unit, option):
    task = task_named(STORAGE_TASKS, "Start Docker and slurmd only with the storage mounted")
    removal = task_named(
        STORAGE_TASKS, "Remove the storage condition of units Pulsar no longer uses"
    )
    [item] = [i for i in resolve_text(task["loop"], pulsar_docker="true") if i["unit"] == unit]
    check = "/usr/local/libexec/pulsar-src/check_storage.py"
    content = resolve_text(task["ansible.builtin.copy"]["content"], item=item)

    assert resolve_text(task["ansible.builtin.copy"]["dest"], item=item) == (
        f"/etc/systemd/system/{unit}.service.d/pulsar-storage.conf"
    )
    assert "RequiresMountsFor" not in content
    assert "StartLimitIntervalSec=0\n" in content
    assert f"ExecStartPre=/usr/bin/python3 -I {check} {option}{STORAGE}\n" in content
    assert "RestartSec=10\n" in content
    # Docker keeps its own Restart=always; slurmd creates its spool if it is missing.
    assert ("Restart=on-failure\n" in content) is (unit == "slurmd")
    spool = f"ExecStartPre=/usr/bin/mkdir -p {STORAGE}/slurmd\n"
    assert (spool in content) is (unit == "slurmd")
    assert task["notify"] == removal["notify"] == "Reload systemd units"
    for docker, slurm in (("true", "slurm"), ("false", "local")):
        variables = {"pulsar_docker": docker, "pulsar_job_runner": slurm}
        [item] = [i for i in resolve_text(task["loop"], **variables) if i["unit"] == unit]
        used = item["enabled"]
        assert condition(task["when"], item=item, **variables) is used
        assert condition(removal["when"], item=item, **variables) is not used


def test_docker_starts_with_its_data_on_the_storage():
    # Written before geerlingguy.docker installs Docker, as the role writes it.
    task = task_named(STORAGE_TASKS, "Put Docker's data on the storage before Docker is installed")
    module = task["ansible.builtin.copy"]
    assert module["dest"] == "/etc/docker/daemon.json"
    assert module["content"] == "{{ docker_daemon_options | to_nice_json }}"
    assert condition(task["when"], pulsar_docker="true") is True
    assert condition(task["when"], pulsar_docker="false") is False
