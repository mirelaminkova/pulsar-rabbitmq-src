"""The playbook under SRC's Ansible: CI runs it with the version SRC's external plugin uses,
and it reads facts through ``ansible_facts`` (top-level fact variables, INJECT_FACTS_AS_VARS,
warn from ansible-core 2.20, as SRC's 2.21 shows, and go away in 2.24)."""

import re

import pytest

from tests.helpers import REPO, load_yaml

# remote_ansible_version of SRC's external Ansible plugin: ansible-core 2.21.
SRC_ANSIBLE = "ansible==14.6.0"

# What the playbook runs: its play, task files, templates, variables and molecule's playbooks.
PLAYBOOK_FILES = sorted(
    path.relative_to(REPO).as_posix()
    for pattern in [
        "playbook.yml",
        "tasks/*.yml",
        "templates/*",
        "group_vars/*.yml",
        "molecule/default/*.yml",
    ]
    for path in REPO.glob(pattern)
)

# Ansible's special and connection variables with an ansible_ prefix that are not facts; add
# one here when the playbook starts to use it. Comments count too: name facts without the prefix.
NOT_FACTS = {
    "ansible_facts",
    "ansible_check_mode",
    "ansible_diff_mode",
    "ansible_playbook_python",
    "ansible_inventory_sources",
    "ansible_play_hosts",
    "ansible_play_hosts_all",
    "ansible_run_tags",
    "ansible_skip_tags",
    "ansible_version",
    "ansible_loop",
    "ansible_managed",
    "ansible_host",
    "ansible_connection",
    "ansible_python_interpreter",
}


def dev_ansible_core():
    """requirements-dev.txt's ansible-core requirement: the unit tests' and contributors'."""
    [line] = [
        line
        for line in (REPO / "requirements-dev.txt").read_text().splitlines()
        if line.startswith("ansible-core")
    ]
    return line


def test_playbook_files_are_found():
    assert {"playbook.yml", "tasks/pulsar.yml", "templates/welcome.html.j2"} <= set(PLAYBOOK_FILES)
    assert "group_vars/all.yml" in PLAYBOOK_FILES


@pytest.mark.parametrize("path", PLAYBOOK_FILES)
def test_facts_are_read_through_ansible_facts(path):
    names = set(re.findall(r"\bansible_\w+", (REPO / path).read_text()))
    assert names <= NOT_FACTS, f"use ansible_facts[...] for {sorted(names - NOT_FACTS)}"


@pytest.mark.parametrize("workflow", ["slurm-live.yml", "molecule.yml"])
def test_ci_runs_the_playbook_with_srcs_ansible(workflow):
    [job] = load_yaml(f".github/workflows/{workflow}")["jobs"].values()
    scripts = [step["run"] for step in job["steps"] if "run" in step]

    assert SRC_ANSIBLE in job["strategy"]["matrix"]["ansible"]
    # The pip requirement goes through the environment, not into the script.
    assert job["env"]["ANSIBLE"] == "${{ matrix.ansible }}"
    assert any("pip install" in script and '"$ANSIBLE"' in script for script in scripts)
    assert not [script for script in scripts if "matrix.ansible" in script]
    # requirements-dev.txt's ansible-core never lands next to it, only in its own virtualenv.
    assert not [
        script
        for script in scripts
        if "requirements-dev" in script and '"$MOLECULE_TESTS_PYTHON" -m pip' not in script
    ]


@pytest.mark.parametrize("workflow", ["slurm-live.yml", "molecule.yml"])
def test_ci_runs_the_playbook_with_the_test_tooling_ansible_as_well(workflow):
    # requirements-dev.txt's ansible-core: the version contributors run the playbook with.
    dev = dev_ansible_core()
    [job] = load_yaml(f".github/workflows/{workflow}")["jobs"].values()

    assert dev in job["strategy"]["matrix"]["ansible"]


def test_pre_commit_hooks_use_the_test_tooling_ansible():
    dev = dev_ansible_core()
    pins = re.findall(r"ansible-core==([\d.]+)", (REPO / ".pre-commit-config.yaml").read_text())

    assert len(pins) == 2
    assert {f"ansible-core~={pin}" for pin in pins} == {dev}


def test_molecule_renders_galaxys_configuration_with_the_test_tooling():
    # tests/helpers.py needs requirements-dev.txt's ansible-core, not the playbook's.
    verify = load_yaml("molecule/default/verify.yml")
    [render] = [
        t
        for play in verify
        for t in play.get("tasks", [])  # import_playbook plays have none
        if t.get("name") == "Render Galaxy's runner and destination for this Pulsar"
    ]
    [job] = load_yaml(".github/workflows/molecule.yml")["jobs"].values()

    assert "MOLECULE_TESTS_PYTHON" in render["ansible.builtin.command"]["argv"][0]
    assert job["env"]["MOLECULE_TESTS_PYTHON"] == "${{ github.workspace }}/.venv/bin/python"
