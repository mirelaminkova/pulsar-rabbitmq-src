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

REPO = Path(__file__).resolve().parents[1]
GALAXY_CONTRACT = REPO / "tests" / "galaxy_contract"

add_all_plugin_dirs(str(REPO))

# Facts the variables and templates use, as on an SRC Ubuntu workspace.
FACTS = {
    "ansible_fqdn": "pulsar.example.org",
    "ansible_hostname": "pulsar",
    "ansible_processor_vcpus": 4,
    "ansible_distribution": "Ubuntu",
    "ansible_distribution_version": "24.04",
    "inventory_hostname": "localhost",
    "workspace_fqdn": "pulsar.example.org",
}

PASSWORD = "Secret.Pass_word~123"  # noqa: S105 - test value
# Component parameters as SRC passes them: strings, empty when unset.
PARAMS = {
    "pulsar_galaxy_fqdn": "galaxy.example.org",
    "pulsar_amqp_port": "5671",
    "pulsar_amqp_name": "hpc",
    "pulsar_amqp_password": PASSWORD,
    "pulsar_manager": "_default_",
    "pulsar_staging_dir": "/mnt/pulsar/files/staging",
    "pulsar_cvmfs": "true",
}


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


def render(template, **params):
    """Render ``templates/<template>`` like ansible.builtin.template (trim_blocks)."""
    text = (REPO / "templates" / template).read_text()
    env = templar(**params).copy_with_new_env(trim_blocks=True)
    return env.do_template(text, preserve_trailing_newlines=True, escape_backslashes=False)


def render_yaml(template, **params):
    return yaml.safe_load(render(template, **params))


def load_yaml(path):
    return yaml.safe_load((REPO / path).read_text())
