"""The playbook under SRC's Ansible: CI runs it with the version SRC's external plugin uses,
and it reads facts through ``ansible_facts`` (top-level fact variables, INJECT_FACTS_AS_VARS,
go away in ansible-core 2.24 and warn from 2.20)."""

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
        "templates/*.j2",
        "group_vars/*.yml",
        "molecule/default/*.yml",
    ]
    for path in REPO.glob(pattern)
)

# Ansible's special variables with an ansible_ prefix that are not facts.
NOT_FACTS = {
    "ansible_facts",
    "ansible_check_mode",
    "ansible_diff_mode",
    "ansible_playbook_python",
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
    assert any('pip install' in script and '"$ANSIBLE"' in script for script in scripts)
    assert not [script for script in scripts if "matrix.ansible" in script]
    # Nothing installs requirements-dev.txt's ansible-core next to it.
    assert not [script for script in scripts if "requirements-dev" in script and "$ANSIBLE" in script]


@pytest.mark.parametrize("workflow", ["slurm-live.yml", "molecule.yml"])
def test_ci_runs_the_playbook_with_the_test_tooling_ansible_as_well(workflow):
    # requirements-dev.txt's ansible-core: the version contributors run the playbook with.
    [dev] = [
        line
        for line in (REPO / "requirements-dev.txt").read_text().splitlines()
        if line.startswith("ansible-core")
    ]
    [job] = load_yaml(f".github/workflows/{workflow}")["jobs"].values()

    assert dev in job["strategy"]["matrix"]["ansible"]


def test_molecule_renders_galaxys_configuration_with_the_test_tooling():
    # tests/helpers.py needs requirements-dev.txt's ansible-core, not the playbook's.
    verify = load_yaml("molecule/default/verify.yml")
    [render] = [
        t
        for play in verify
        for t in play["tasks"]
        if t.get("name") == "Render Galaxy's runner and destination for this Pulsar"
    ]
    [job] = load_yaml(".github/workflows/molecule.yml")["jobs"].values()

    assert "MOLECULE_TESTS_PYTHON" in render["ansible.builtin.command"]["argv"][0]
    assert job["env"]["MOLECULE_TESTS_PYTHON"] == "${{ github.workspace }}/.venv/bin/python"
