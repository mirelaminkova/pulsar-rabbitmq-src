"""Galaxy's job runner and destination for this Pulsar, from the Galaxy component's own files.

The Galaxy component (src-component-galaxy-amqp) turns a ``src_galaxy_pulsar_amqp_configs``
entry into a ``PulsarMQJobRunner`` runner (``templates/galaxy/config/job_conf.yml.j2``) and a
TPV destination (``files/galaxy/tpv/destinations.yml.j2``). This renders the vendored copies of
both (tests/galaxy_contract/) from Galaxy's parse of the entry, so the end-to-end job uses
exactly the parameters a real Galaxy would.

    python -m tests.e2e.galaxy_config PARAMS_YML GALAXY_URL > galaxy_job.json

takes this component's parameters (pulsar_*, as SRC passes them) from PARAMS_YML, derives the
entry the welcome page shows from them with the playbook's own variables, reads the RabbitMQ
password of Galaxy's own ``galaxy`` user from the environment variable GALAXY_AMQP_PASSWORD
(never from the command line) and prints the JSON that tests/e2e/galaxy_job.py takes.
"""

import json
import os
import sys

import jinja2
import yaml
from ansible.plugins.filter.core import FilterModule

from tests.galaxy_contract import src_galaxy
from tests.helpers import GALAXY_CONTRACT, resolve

# Galaxy's _galaxy_rabbitmq_local_port: plain AMQP on the Galaxy host's loopback interface.
GALAXY_RABBITMQ_LOCAL_PORT = 5672

# Galaxy's settings the two templates use besides the Pulsar entries; none of them affects an
# AMQP Pulsar's runner or destination.
_UNRELATED = {
    "_galaxy_has_pulsar": False,
    "_galaxy_byoc_runners": [],
    "_galaxy_pulsar_configs_raw": [],
    "_galaxy_jobs_default": "local",
    "_galaxy_use_interactive_tools": False,
    "_galaxy_tpv_config_dir": "/srv/galaxy/config/tpv",
    "galaxy_mutable_data_dir": "/srv/galaxy/var",
    "apptainer_volumes": [],
}


def _render(template, **variables):
    # Renders YAML, not HTML, like Ansible's template module: no autoescaping.
    env = jinja2.Environment(  # noqa: S701  # nosemgrep: missing-autoescape-disabled
        undefined=jinja2.StrictUndefined, trim_blocks=True
    )
    env.filters.update(FilterModule().filters())
    text = env.from_string((GALAXY_CONTRACT / template).read_text()).render(**variables)
    return yaml.safe_load(text)


def galaxy_job_config(entry, galaxy_password, galaxy_url):
    """Return ``{"runner": ..., "destination": ...}`` for the one Pulsar in ``entry``.

    ``entry`` is this Pulsar's ``src_galaxy_pulsar_amqp_configs`` value (the welcome page's);
    ``galaxy_url`` is Galaxy's ``galaxy_infrastructure_url``, where Pulsar fetches inputs and
    posts outputs.
    """
    [parsed] = src_galaxy.src_config_list(entry, "src_galaxy_pulsar_amqp_configs")
    errors = src_galaxy.pulsar_amqp_config_errors([parsed])
    if errors:
        raise ValueError("; ".join(errors))
    [pulsar] = src_galaxy.pulsar_amqp_configs([parsed])
    variables = {
        **_UNRELATED,
        "_galaxy_pulsar_amqp_configs": [pulsar],
        "_galaxy_rabbitmq_passwords": {"galaxy": galaxy_password},
        "_galaxy_rabbitmq_local_port": GALAXY_RABBITMQ_LOCAL_PORT,
        "_galaxy_infrastructure_url": galaxy_url,
    }
    runner = _render("job_conf.yml.j2", **variables)["runners"][pulsar["runner"]]
    destination = _render("destinations.yml.j2", **variables)["destinations"]
    return {"runner": runner, "destination": destination[pulsar["environment"]]["params"]}


def main(argv=None):
    params_yml, galaxy_url = sys.argv[1:] if argv is None else argv
    with open(params_yml) as handle:
        params = yaml.safe_load(handle)
    # The welcome page's entry: _pulsar_amqp.galaxy_entry from group_vars/all.yml.
    entry = resolve("_pulsar_amqp", **params)["galaxy_entry"]
    config = galaxy_job_config(entry, os.environ["GALAXY_AMQP_PASSWORD"], galaxy_url)
    json.dump(config, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":  # pragma: no cover
    main()
