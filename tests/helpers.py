"""Resolve group_vars and render templates with ansible-core, as the playbook would.

The playbook's filter plugins (``filter_plugins/``) are registered like
``ansible-playbook`` does for the playbook directory, so variables, filters and
templates are tested together.
"""

from pathlib import Path

import yaml
from ansible.parsing.dataloader import DataLoader
from ansible.plugins.loader import add_all_plugin_dirs
from ansible.template import Templar

from filter_plugins.pulsar_amqp import pulsar_amqp_settings

REPO = Path(__file__).resolve().parents[1]
GALAXY_CONTRACT = REPO / "tests" / "galaxy_contract"

add_all_plugin_dirs(str(REPO))

# Facts the variables and templates use (through ansible_facts), as on an SRC Ubuntu workspace.
FACTS = {
    "ansible_facts": {
        "fqdn": "pulsar.example.org",
        "hostname": "pulsar",
        "processor_vcpus": 4,
        "memtotal_mb": 16000,
        "architecture": "x86_64",
        "distribution": "Ubuntu",
        "distribution_version": "24.04",
        "distribution_release": "noble",
    },
    "inventory_hostname": "localhost",
    "workspace_fqdn": "pulsar.example.org",
    # Ansible's placeholder for a left-out module argument (always defined).
    "omit": "__omit_place_holder__test",
}

PASSWORD = "Secret.Pass_word~123"  # noqa: S105 - test value
# Component parameters as SRC passes them: strings, empty when unset.
PARAMS = {
    "pulsar_galaxy_fqdn": "galaxy.example.org",
    "pulsar_amqp_port": "5671",
    "pulsar_amqp_name": "hpc",
    "pulsar_amqp_password": PASSWORD,
    "pulsar_manager": "_default_",
    "pulsar_storage_path": "/data/pulsar_storage/pulsar",
    "pulsar_cvmfs": "true",
    "pulsar_job_runner": "slurm",
}


def facts(**overrides):
    """``ansible_facts`` of FACTS with ``overrides``, as a parameter of templar() and friends."""
    return {"ansible_facts": {**FACTS["ansible_facts"], **overrides}}


def templar(**params):
    """Return a Templar with group_vars/all.yml, FACTS and PARAMS overridden by ``params``."""
    loader = DataLoader()
    variables = loader.load_from_file(str(REPO / "group_vars" / "all.yml"))
    variables.update(FACTS)
    variables.update(PARAMS)
    variables.update(params)
    return Templar(loader=loader, variables=variables)


def resolve(name, **params):
    """Return variable ``name`` as the playbook sees it."""
    return templar(**params).template(f"{{{{ {name} }}}}", convert_data=True)


def resolve_text(text, **params):
    """Template a string such as a task argument, as the playbook would."""
    return templar(**params).template(text)


def render(template, **params):
    """Render ``templates/<template>`` like ansible.builtin.template (trim_blocks)."""
    text = (REPO / "templates" / template).read_text()
    env = templar(**params).copy_with_new_env(trim_blocks=True)
    return env.do_template(text, preserve_trailing_newlines=True, escape_backslashes=False)


def render_yaml(template, **params):
    return yaml.safe_load(render(template, **params))


def load_yaml(path):
    return yaml.safe_load((REPO / path).read_text())


def tasks(path):
    """All tasks of a task file (or a list of tasks), including those inside blocks."""
    task_list = load_yaml(path) if isinstance(path, str) else path
    return [t for task in task_list or [] for t in [task, *task.get("block", [])]]


def play():
    [play] = load_yaml("playbook.yml")
    return play


def task_named(task_list, name):
    [task] = [t for t in task_list if t.get("name") == name]
    return task


def condition(expression, **variables):
    """Evaluate a task's ``when`` expression with ``variables``. Unlike Ansible, the
    result goes through ``| bool``; fine for expressions that yield booleans."""
    return templar(**variables).template(f"{{{{ ({expression}) | bool }}}}", convert_data=True)


# The filter's parameters (not group_vars') for a valid Pulsar.
BASE = {
    "galaxy_fqdn": PARAMS["pulsar_galaxy_fqdn"],
    "port": PARAMS["pulsar_amqp_port"],
    "name": PARAMS["pulsar_amqp_name"],
    "password": PARAMS["pulsar_amqp_password"],
    "manager": PARAMS["pulsar_manager"],
    "storage_path": PARAMS["pulsar_storage_path"],
    "docker_enabled": "true",
    "singularity_enabled": "true",
}


def settings(**overrides):
    """pulsar_amqp_settings of BASE with ``overrides``."""
    return pulsar_amqp_settings({**BASE, **overrides})
