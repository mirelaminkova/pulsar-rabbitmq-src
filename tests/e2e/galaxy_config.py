"""Galaxy's job runner and destination for this Pulsar, from byoc-sync's own templates.

byoc-sync (sanjaysrikakulam/elixir_biohackathon_2026_project1, byoc-sync/) turns the
``byoc_pulsar`` form of a user's Galaxy preferences into a ``PulsarMQJobRunner`` runner
(``templates/eu/runner.yml.j2``) and a TPV destination (``templates/eu/destination.yml.j2``).
This renders the vendored copies of both (tests/byoc_contract/) from this component's
settings, so the end-to-end job uses exactly the parameters a real Galaxy would.

    python -m tests.e2e.galaxy_config PARAMS_YML GALAXY_URL > galaxy_job.json

takes this component's parameters (pulsar_*, as SRC passes them; the name and password must
be the ones the deployment used) from PARAMS_YML, derives the form and the names from them
with the playbook's own variables and prints the JSON that tests/e2e/galaxy_job.py takes:
the runner's parameters (with the password in ``amqp_url``, as in Galaxy's job_conf.yml) and
the destination's. tests/test_byoc_contract.py renders the same templates through this
module to compare app.yml with the runner.
"""

import argparse
import json
import sys

import jinja2
import yaml

from filter_plugins.pulsar_amqp import pulsar_amqp_errors
from tests.byoc_contract import naming
from tests.helpers import BYOC_CONTRACT, resolve

SITE = yaml.safe_load((BYOC_CONTRACT / "eu.yml").read_text())
# The destination's params the job needs (its limits and the owner guard are Galaxy's).
DESTINATION_PARAMS = ("jobs_directory", "persistence_directory")


def byoc_names(name):
    """byoc-sync's names for the BYOC ``name`` (usegalaxy.eu's patterns)."""
    return naming.Names.for_(name, naming.NamingConfig(**SITE["naming"]))


def render_byoc_template(template, **context):
    """Render a byoc-sync template as its Renderer does ([[ ]] delimiters, its filters)."""
    env = jinja2.Environment(  # noqa: S701  # nosemgrep: missing-autoescape-disabled
        loader=jinja2.FileSystemLoader(str(BYOC_CONTRACT / "templates" / "eu")),
        variable_start_string="[[",
        variable_end_string="]]",
        block_start_string="[%",
        block_end_string="%]",
        comment_start_string="[#",
        comment_end_string="#]",
        undefined=jinja2.StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["q"] = lambda v: json.dumps(str(v))
    env.filters["pystr"] = lambda v: json.dumps(str(v))
    return env.get_template(template).render(**context)


def galaxy_job_config(params, galaxy_url):
    """Return ``{"runner": ..., "destination": ...}`` for the Pulsar of ``params``.

    ``params`` are this component's parameters (``pulsar_*``); ``galaxy_url`` is Galaxy's
    ``galaxy_infrastructure_url``, where Pulsar fetches inputs and posts outputs.
    """
    amqp = resolve("_pulsar_amqp", **params)
    errors = pulsar_amqp_errors(amqp)
    if errors:
        raise ValueError("; ".join(errors))
    if not (amqp["name"] and amqp["password"]):
        raise ValueError("pulsar_amqp_name and pulsar_amqp_password must be the deployment's")
    form = resolve("_pulsar_byoc_form", **params)
    runner = galaxy_runner(amqp, form, galaxy_url)
    destination = galaxy_destination(amqp, form)
    return {
        "runner": {"load": runner["load"], **runner["params"]},
        "destination": {key: destination["params"][key] for key in DESTINATION_PARAMS},
    }


def byoc_request(form):
    """byoc-sync's ``req`` for the form: a ByocRequest's fields except name and password."""
    return {
        "max_cores": form["max_accepted_cores"],
        "max_mem": form["max_accepted_mem"],
        "min_gpus": form["min_accepted_gpus"],
        "max_gpus": form["max_accepted_gpus"],
        "nfs_path": form["nfs_path"],
    }


def galaxy_runner(amqp, form, galaxy_url=SITE["site"]["galaxy_url"]):
    """Galaxy's runner entry for this Pulsar, from byoc-sync's runner.yml.j2.

    ``galaxy_url`` defaults to usegalaxy.eu's, for the contract tests.
    """
    names = byoc_names(amqp["name"])
    site = {"mq_host": amqp["host"], "mq_port": amqp["port"], "galaxy_url": galaxy_url}
    text = render_byoc_template("runner.yml.j2", names=names, site=site, req=byoc_request(form))
    # byoc-sync writes '{{ rabbitmq_password_... }}' into the runner for Ansible's vault
    # to fill in when the playbook is deployed; here the password takes its place.
    [runner] = yaml.safe_load(text.replace("{{ " + names.vault_var + " }}", amqp["password"]))
    return runner


def galaxy_destination(amqp, form):
    """Galaxy's TPV destination for this Pulsar, from byoc-sync's destination.yml.j2."""
    text = render_byoc_template(
        "destination.yml.j2",
        names=byoc_names(amqp["name"]),
        req=byoc_request(form),
        email="e2e@example.org",
    )
    [destination] = yaml.safe_load(text).values()
    return destination


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("params_yml", help="this component's parameters (pulsar_*)")
    parser.add_argument("galaxy_url", help="Galaxy's galaxy_infrastructure_url")
    args = parser.parse_args(argv)
    with open(args.params_yml) as handle:
        params = yaml.safe_load(handle)
    config = galaxy_job_config(params, args.galaxy_url)
    json.dump(config, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":  # pragma: no cover
    main()
