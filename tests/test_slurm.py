"""Pulsar's jobs through a single-node Slurm with DRMAA (pulsar_job_runner), or locally.

Galaxy sees none of this: its entry, the manager name and the queues stay the same
for both runners (also tests/test_galaxy_contract.py).
"""

import os
import re
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pulsar.manager_factory import build_managers
from pulsar.managers.queued import QueueManager
from pulsar.managers.queued_drmaa import DrmaaQueueManager

from filter_plugins.pulsar_amqp import pulsar_amqp_errors, pulsar_amqp_settings
from tests.helpers import (
    FACTS,
    PARAMS,
    REPO,
    load_yaml,
    named,
    render,
    render_yaml,
    resolve,
    resolve_text,
    task_list,
    templar,
    the_play,
)

DRMAA_LIBRARY = "/usr/lib/slurm-drmaa/lib/libdrmaa.so.1"
OMIT = FACTS["omit"]


BASE = {
    "galaxy_fqdn": PARAMS["pulsar_galaxy_fqdn"],
    "name": PARAMS["pulsar_amqp_name"],
    "password": PARAMS["pulsar_amqp_password"],
}


def settings(**overrides):
    return pulsar_amqp_settings({**BASE, **overrides})


# --- Parameter ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", "slurm"), (None, "slurm"), ("slurm", "slurm"), (" Local ", "local"), ("SLURM", "slurm")],
)
def test_job_runner_values(value, expected):
    result = settings(job_runner=value)

    assert result["job_runner"] == expected
    assert pulsar_amqp_errors(result) == []


@pytest.mark.parametrize("value", ["drmaa", "queued_python", "true", "slurm local"])
def test_unknown_job_runner_is_an_error(value):
    errors = pulsar_amqp_errors(settings(job_runner=value))

    assert len(errors) == 1, errors
    assert "pulsar_job_runner must be slurm or local" in errors[0]
    assert value in errors[0]


def test_galaxy_entry_does_not_depend_on_the_runner():
    assert (
        settings(job_runner="slurm")["galaxy_entry"] == settings(job_runner="local")["galaxy_entry"]
    )
    assert "slurm" not in settings()["galaxy_entry"]


def test_parameter_defaults_to_slurm():
    assert load_yaml("parameters.yml")["pulsar_job_runner"] == "slurm"
    assert load_yaml("group_vars/all.yml")["pulsar_job_runner"] == "slurm"
    assert resolve("_pulsar_slurm") is True
    assert resolve("_pulsar_slurm", pulsar_job_runner="") is True
    assert resolve("_pulsar_slurm", pulsar_job_runner="local") is False


def test_group_vars_report_a_wrong_runner():
    [error] = resolve("_pulsar_amqp_errors", pulsar_job_runner="sge")
    assert "pulsar_job_runner" in error


# --- app.yml -----------------------------------------------------------------


@pytest.mark.parametrize("manager", ["_default_", "gpu"])
def test_slurm_runner_uses_queued_drmaa_under_galaxys_manager_name(manager):
    app = render_yaml("pulsar-app.yml.j2", pulsar_manager=manager)

    assert app["managers"] == {manager: {"type": "queued_drmaa", "min_polling_interval": 0.5}}


def test_local_runner_keeps_queued_python():
    app = render_yaml("pulsar-app.yml.j2", pulsar_job_runner="local")

    assert app["managers"] == {
        "_default_": {
            "type": "queued_python",
            "num_concurrent_jobs": 3,
            "min_polling_interval": 0.5,
        }
    }


class _FakeDrmaaFactory:
    def get(self):
        return None


@pytest.mark.parametrize(
    ("runner", "manager_class"), [("slurm", DrmaaQueueManager), ("local", QueueManager)]
)
def test_pulsar_builds_the_manager_from_our_app_yml(runner, manager_class, tmp_path, monkeypatch):
    app_yml = render_yaml("pulsar-app.yml.j2", pulsar_job_runner=runner)
    # No libdrmaa here: the live CI job submits through the real library.
    monkeypatch.setattr(
        "pulsar.managers.base.base_drmaa.DrmaaSessionFactory", _FakeDrmaaFactory, raising=True
    )
    workers = []
    monkeypatch.setattr(QueueManager, "_init_worker_threads", lambda _, n: workers.append(n))
    app = SimpleNamespace(
        staging_directory=str(tmp_path),
        persistence_directory=None,
        authorizer=None,
        user_auth_manager=None,
        dependency_manager=None,
        job_metrics=None,
        object_store=None,
    )

    [(name, proxy)] = build_managers(app, app_yml).items()

    assert name == "_default_"
    assert type(proxy._proxied_manager) is manager_class
    assert proxy.min_polling_interval.total_seconds() == 0.5
    assert workers == ([3] if runner == "local" else [])


# --- systemd unit ------------------------------------------------------------


def test_unit_points_pulsar_at_the_slurm_drmaa_library():
    unit = render("pulsar.service.j2")

    assert f"Environment=DRMAA_LIBRARY_PATH={DRMAA_LIBRARY}\n" in unit
    after = re.search(r"^After=(.*)$", unit, re.M).group(1).split()
    assert {"munge.service", "slurmctld.service"} <= set(after)


def test_local_unit_has_no_slurm():
    unit = render("pulsar.service.j2", pulsar_job_runner="local")

    assert "DRMAA" not in unit
    assert "slurm" not in unit


# --- Slurm settings for galaxyproject.slurm -----------------------------------


def test_controller_and_compute_node_are_this_workspace():
    assert resolve("slurm_roles") == ["controller", "exec"]
    [node] = resolve("slurm_nodes")
    assert node["name"] == "localhost"
    [partition] = resolve("slurm_partitions")
    assert partition["Nodes"] == "localhost"
    assert partition["Default"] is True
    assert resolve("slurm_config")["SlurmctldHost"] == "localhost"


@pytest.mark.parametrize(
    ("memtotal", "vcpus", "memory", "per_cpu"),
    [
        (16000, 4, 14400, 3600),  # 10 % kept
        (64000, 16, 57600, 3600),
        (4000, 2, 2976, 1488),  # at least 1 GiB kept
        (2048, 1, 1024, 1024),
        (1500, 1, 750, 750),  # never more than half kept
    ],
)
def test_node_size_comes_from_the_workspace(memtotal, vcpus, memory, per_cpu):
    facts = {"ansible_memtotal_mb": memtotal, "ansible_processor_vcpus": vcpus}
    [node] = resolve("slurm_nodes", **facts)
    config = resolve("slurm_config", **facts)

    assert (int(node["CPUs"]), int(node["RealMemory"])) == (vcpus, memory)
    # Slurm drains a node that reports less memory than RealMemory.
    assert int(node["RealMemory"]) < memtotal
    # A job without --mem gets its share, so every CPU can run a job.
    assert int(config["DefMemPerCPU"]) == per_cpu
    assert int(config["DefMemPerCPU"]) * vcpus <= int(node["RealMemory"])


def test_jobs_share_the_node_by_cpu_and_memory():
    # Ubuntu 22.04's Slurm 21.08 defaults to select/linear: one job per node.
    config = resolve("slurm_config")

    assert config["SelectType"] == "select/cons_tres"
    assert config["SelectTypeParameters"] == "CR_CPU_Memory"
    assert int(config["ReturnToService"]) == 2
    # Without accounting, finished jobs stay known this long (four hours).
    assert int(config["MinJobAge"]) == 14400
    assert "MaxJobCount" not in config  # the default 10000 covers four hours of jobs


def test_epilog_only_with_docker():
    with_docker = resolve("slurm_config", pulsar_docker="true", omit=OMIT)
    without = resolve("slurm_config", pulsar_docker="false", omit=OMIT)

    assert with_docker["Epilog"] == "/etc/slurm/epilog.sh"
    assert int(with_docker["PrologEpilogTimeout"]) == 90
    assert without["Epilog"] == OMIT


def _role_dir():
    paths = [
        *os.environ.get("ANSIBLE_ROLES_PATH", "").split(":"),
        str(Path.home() / ".ansible" / "roles"),
        "/usr/share/ansible/roles",
        "/etc/ansible/roles",
    ]
    for path in paths:
        role = Path(path) / "galaxyproject.slurm"
        if path and (role / "templates" / "slurm.conf.j2").exists():
            return role
    message = "galaxyproject.slurm is not installed: ansible-galaxy install -r requirements.yml"
    if os.environ.get("CI"):
        pytest.fail(message)
    pytest.skip(message)


@pytest.mark.parametrize("docker", ["true", "false"])
@pytest.mark.parametrize("ubuntu", ["22", "24"])
def test_slurm_conf_as_the_role_writes_it(docker, ubuntu):
    role = _role_dir()
    defaults = yaml.safe_load((role / "defaults" / "main.yml").read_text())
    variables = {
        "ansible_distribution_major_version": ubuntu,
        "ansible_os_family": "Debian",
        "group_names": [],
        "pulsar_docker": docker,
    }
    # Role defaults are below group_vars.
    group_vars = load_yaml("group_vars/all.yml")
    defaults = {key: value for key, value in defaults.items() if key not in group_vars}
    env = templar(**{**defaults, **variables})
    conf = env.copy_with_new_env(trim_blocks=True).do_template(
        (role / "templates" / "slurm.conf.j2").read_text(), preserve_trailing_newlines=True
    )
    lines = [line for line in conf.splitlines() if line and not line.startswith("#")]

    assert "SlurmctldHost=localhost" in lines
    assert "ControlMachine=localhost" not in lines
    assert "NodeName=localhost CPUs=4 RealMemory=14400 State=UNKNOWN" in lines
    assert "PartitionName=pulsar Default=YES MaxTime=INFINITE Nodes=localhost State=UP" in lines
    assert "SelectTypeParameters=CR_CPU_Memory" in lines
    assert "DefMemPerCPU=3600" in lines
    assert "MinJobAge=14400" in lines
    assert ("Epilog=/etc/slurm/epilog.sh" in lines) is (docker == "true")
    assert not [line for line in lines if OMIT in line]
    # The epilog lives in the role's slurm_config_dir on both Ubuntu releases.
    assert env.template("{{ slurm_config_dir }}") == str(
        Path(resolve("pulsar_slurm_epilog")).parent
    )


# --- Playbook ----------------------------------------------------------------


def test_slurm_is_set_up_before_pulsar_and_stopped_for_local():
    names = [t.get("name") for t in the_play()["tasks"]]
    slurm = named(the_play()["tasks"], "Run Pulsar's jobs through Slurm on this workspace")
    off = named(the_play()["tasks"], "Stop Slurm when Pulsar runs jobs itself")
    pulsar = "Install Pulsar and connect it to Galaxy's RabbitMQ"

    assert slurm["ansible.builtin.include_tasks"] == "tasks/slurm.yml"
    assert slurm["when"] == "_pulsar_slurm | bool"
    assert names.index(slurm["name"]) < names.index(pulsar) < names.index(off["name"])
    assert off["when"] == "not _pulsar_slurm | bool"


def test_pulsar_restarts_for_a_new_manager_type_or_unit_environment():
    pulsar = task_list("tasks/pulsar.yml")
    [handler] = the_play()["handlers"]

    for name in (
        "Write Pulsar's app.yml with the Galaxy broker connection",  # manager type
        "Install the Pulsar systemd unit",  # DRMAA_LIBRARY_PATH
    ):
        assert named(pulsar, name)["notify"] == handler["name"] == "Restart Pulsar"
    assert handler["ansible.builtin.systemd"]["daemon_reload"] is True
    assert handler["ansible.builtin.systemd"]["state"] == "restarted"


def test_local_runner_stops_slurm_if_installed():
    [facts, stop] = task_list("tasks/slurm-off.yml")

    assert "ansible.builtin.service_facts" in facts
    assert stop["ansible.builtin.systemd"] == {
        "name": "{{ item }}",
        "state": "stopped",
        "enabled": False,
    }
    assert stop["loop"] == ["slurmctld", "slurmd"]
    assert stop["when"] == "(item ~ '.service') in ansible_facts.services"


def test_slurm_tasks():
    slurm = task_list("tasks/slurm.yml")
    names = [t["name"] for t in slurm]
    role = named(slurm, "Install Slurm as controller and compute node")
    packages = named(slurm, "Install the Slurm DRMAA library")
    epilog = named(slurm, "Install the epilog that stops a job's Docker container")
    node = named(slurm, "Wait until the Slurm node accepts jobs")

    assert role["ansible.builtin.include_role"]["name"] == "galaxyproject.slurm"
    names_ = packages["ansible.builtin.apt"]["name"]
    assert resolve_text(names_, pulsar_docker="true") == ["slurm-drmaa1", "jq"]
    assert resolve_text(names_, pulsar_docker="false") == ["slurm-drmaa1"]
    assert epilog["ansible.builtin.copy"] == {
        "src": "files/epilog.sh",
        "dest": "{{ pulsar_slurm_epilog }}",
        "owner": "root",
        "group": "root",
        "mode": "0755",
    }
    assert epilog["when"] == "_pulsar_amqp.docker_enabled"
    assert names.index(role["name"]) < names.index(epilog["name"])
    # Restart Slurm with a changed slurm.conf before checking the node.
    assert names.index("Apply pending Slurm restarts") < names.index(node["name"])
    assert node["changed_when"] is False
    assert node["until"] == "_pulsar_slurm_node.stdout | trim in _pulsar_slurm_ready_states"
    not_ready = "_pulsar_slurm_node.stdout | trim not in _pulsar_slurm_ready_states"
    assert named(slurm, "Show why the Slurm node cannot run jobs")["when"] == not_ready
    assert named(slurm, "Fail when the Slurm node cannot run jobs")["when"] == not_ready
    ready = resolve("_pulsar_slurm_ready_states")
    assert not {"down", "drained", "draining", "fail", "unknown"} & set(ready)


def test_munge_key_is_the_packages_own_and_checked():
    # munge generates a random key per workspace; none is shipped or logged.
    assert "slurm_munge_key" not in load_yaml("group_vars/all.yml")
    check = named(task_list("tasks/slurm.yml"), "Check the munge key")
    assert check["ansible.builtin.stat"]["path"] == "/etc/munge/munge.key"
    assert check["ansible.builtin.stat"]["get_checksum"] is False
    verdict = named(task_list("tasks/slurm.yml"), "Require a private munge key")
    that = " ".join(verdict["ansible.builtin.assert"]["that"])
    assert "pw_name == 'munge'" in that
    assert "mode in ['0400', '0600']" in that


def test_slurm_drmaa_comes_from_the_galaxy_projects_ppa():
    # Ubuntu dropped slurm-drmaa after 16.04; galaxyproject.repos adds this PPA.
    slurm = task_list("tasks/slurm.yml")
    names = [t["name"] for t in slurm]
    key = named(slurm, "Install the slurm-drmaa PPA key")["ansible.builtin.copy"]
    repo = named(slurm, "Add the slurm-drmaa PPA")["ansible.builtin.apt_repository"]

    assert key["src"] == "files/slurm-drmaa.asc"
    assert resolve_text(key["dest"]) == "/usr/share/keyrings/slurm-drmaa.asc"
    assert resolve_text(repo["repo"], ansible_distribution_release="noble") == (
        "deb [signed-by=/usr/share/keyrings/slurm-drmaa.asc] "
        "https://ppa.launchpadcontent.net/natefoo/slurm-drmaa/ubuntu noble main"
    )
    assert names.index("Add the slurm-drmaa PPA") < names.index("Install the Slurm DRMAA library")


@pytest.mark.parametrize(
    ("key", "fingerprint"),
    [
        # Launchpad's signing_key_fingerprint of ~natefoo/+archive/ubuntu/slurm-drmaa
        ("slurm-drmaa.asc", "8DE68488997C5C6BA19021136F2CC56412788738"),
        # and of ~apptainer/+archive/ubuntu/ppa.
        ("apptainer.asc", "F6B0F5193D4F3301EF491FF0AFE36534FC6218AE"),
    ],
)
def test_ppa_keys_in_the_repository(key, fingerprint, tmp_path):
    # Shipped, not downloaded: a keyserver's export changes and breaks idempotence.
    gpg = shutil.which("gpg")
    if not gpg:
        pytest.skip("gpg is not installed")
    shown = subprocess.run(  # noqa: S603 - fixed arguments
        [gpg, "--homedir", str(tmp_path), "--show-keys", "--with-colons"],
        input=(REPO / "files" / key).read_bytes(),
        capture_output=True,
        check=True,
    ).stdout.decode()
    fingerprints = [line.split(":")[9] for line in shown.splitlines() if line.startswith("fpr:")]

    assert fingerprints == [fingerprint]


def test_apptainer_key_is_copied_not_downloaded():
    [key] = [t for t in task_list("tasks/apptainer.yml") if "key" in t["name"]]
    assert key["ansible.builtin.copy"]["src"] == "files/apptainer.asc"


def test_drmaa_library_is_checked():
    check = named(task_list("tasks/slurm.yml"), "Check the Slurm DRMAA library")
    assert check["ansible.builtin.stat"]["path"] == "{{ pulsar_drmaa_library_path }}"
    assert resolve("pulsar_drmaa_library_path") == DRMAA_LIBRARY


def test_drmaa_python_package_in_pulsars_venv():
    pip = named(task_list("tasks/pulsar.yml"), "Install pulsar-app with AMQP support into the venv")
    assert "drmaa=={{ pulsar_drmaa_version }}" in pip["ansible.builtin.pip"]["name"]


def test_requirements_pin_galaxyproject_slurm():
    roles = {role["name"]: role["version"] for role in load_yaml("requirements.yml")["roles"]}
    assert roles["galaxyproject.slurm"] == "1.0.5"


def test_live_check_can_skip_only_the_broker_and_docker_install():
    # CI's live Slurm job has no Galaxy broker and its own Docker.
    check = named(
        task_list("tasks/pulsar.yml"),
        "Check that Pulsar consumes its setup queue on Galaxy's broker",
    )
    report = named(task_list("tasks/pulsar.yml"), "Fail when Pulsar cannot use Galaxy's broker")
    [docker] = [r for r in the_play()["roles"] if r["role"] == "geerlingguy.docker"]

    assert check["tags"] == report["tags"] == ["broker-check"]
    assert docker["tags"] == ["docker-install"]


# --- Welcome page ------------------------------------------------------------


def test_welcome_page_describes_slurm():
    page = render("welcome.html.j2")

    assert "Slurm" in page
    assert "<code>queued_drmaa</code>" in page
    assert "4 CPUs and 14400 MB" in page
    for command in ("squeue", "scontrol show job", "sinfo"):
        assert command in page


def test_welcome_page_describes_the_local_runner():
    page = render("welcome.html.j2", pulsar_job_runner="local")

    assert "3 concurrent jobs" in page
    assert "squeue" not in page
