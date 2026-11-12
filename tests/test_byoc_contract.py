"""This component's settings against byoc-sync's own code (vendored in tests/byoc_contract/).

byoc-sync (sanjaysrikakulam/elixir_biohackathon_2026_project1, byoc-sync/) turns the
``byoc_pulsar`` form of a Galaxy user's preferences into the RabbitMQ account, runner and
destination of their Pulsar. Its naming and validation rules are the contract: the names this
component uses must be the ones byoc-sync creates, the values it accepts must be the ones
byoc-sync accepts, and Pulsar's app.yml must match the runner byoc-sync writes.
"""

import filecmp
import json
import os
from pathlib import Path

import jinja2
import pytest
import yaml

from filter_plugins.pulsar_amqp import pulsar_amqp_errors, pulsar_byoc_form
from tests.byoc_contract import naming, validate
from tests.helpers import BYOC_CONTRACT, PASSWORD, load_yaml, render_yaml, resolve, settings
from tests.test_parameters import INVALID_NAMES, INVALID_PASSWORDS, VALID_NAMES, VALID_PASSWORDS

SITE = yaml.safe_load((BYOC_CONTRACT / "eu.yml").read_text())


def byoc_names(name):
    return naming.Names.for_(name, naming.NamingConfig(**SITE["naming"]))


def byoc_render(template, **context):
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
        keep_trailing_newline=False,
    )
    env.filters["q"] = lambda v: json.dumps(str(v))
    env.filters["pystr"] = lambda v: json.dumps(str(v))
    return env.get_template(template).render(**context)


# Names


@pytest.mark.parametrize("name", VALID_NAMES)
def test_user_and_vhost_are_byoc_syncs(name):
    ours, theirs = settings(name=name), byoc_names(name)

    assert ours["mq_user"] == theirs.mq_user
    assert ours["vhost"] == theirs.vhost
    assert ours["url"].endswith(f"@{ours['host']}:{ours['port']}/{theirs.vhost}")


@pytest.mark.parametrize("name", VALID_NAMES)
def test_byoc_sync_recognises_our_user_as_this_name(name):
    config = naming.NamingConfig(**SITE["naming"])
    ours = settings(name=name)

    assert config.name_from("mq_user", ours["mq_user"]) == name
    assert config.name_from("vhost", ours["vhost"]) == name


def test_site_defaults_are_ours():
    # usegalaxy.eu's broker is this component's default broker.
    result = settings(host="", port="")

    assert result["host"] == SITE["site"]["mq_host"]
    assert result["port"] == SITE["site"]["mq_port"]
    assert naming.NamingConfig(**SITE["naming"]) == naming.NamingConfig()


# Validation: valid here <=> byoc-sync accepts the form value.


def byoc_errors(validator, value):
    errors = {}
    validator(value, errors)
    return errors


@pytest.mark.parametrize("name", VALID_NAMES + INVALID_NAMES)
def test_name_rules_match_byoc_syncs(name):
    ours = [e for e in pulsar_amqp_errors(settings(name=name)) if "pulsar_amqp_name" in e]
    theirs = byoc_errors(validate._name, name)

    assert (ours == []) == (theirs == {}), (ours, theirs)
    assert (ours == []) == validate.is_valid_name(name)


@pytest.mark.parametrize("password", VALID_PASSWORDS + INVALID_PASSWORDS)
def test_password_rules_match_byoc_syncs(password):
    ours = pulsar_amqp_errors(settings(password=password))
    theirs = byoc_errors(validate._password, password)

    assert (ours == []) == (theirs == {}), (ours, theirs)


@pytest.mark.parametrize(
    "storage", ["/data/vol/pulsar", "/data/vol", "/data/vol-2.a_b/x", "/data/" + "a" * 200]
)
def test_the_storage_directory_is_a_valid_nfs_path(storage):
    result = settings(storage_path=storage)

    assert pulsar_amqp_errors(result) == []
    assert byoc_errors(validate._nfs_path, result["storage_path"]) == {}
    assert validate._nfs_path(result["storage_path"], {}) == result["storage_path"]


def test_a_storage_path_too_long_for_byoc_sync_is_refused():
    storage = "/data/vol/" + "a" * 250

    errors = pulsar_amqp_errors(settings(storage_path=storage))

    assert len(errors) == 1, errors
    assert "pulsar_storage_path" in errors[0]
    assert byoc_errors(validate._nfs_path, storage) != {}


def test_the_form_passes_byoc_syncs_validation():
    form = pulsar_byoc_form(settings(), 4, 16000)
    record = validate.UserRecord(
        galaxy_id="1",
        email="alice@example.org",
        username="alice",
        deleted=False,
        purged=False,
        active=True,
        prefs={f"byoc_pulsar|{key}": str(value) for key, value in form.items()}
        | {"byoc_pulsar|password": PASSWORD},
        source="test",
    )

    request = validate.validate(record)

    assert isinstance(request, validate.ByocRequest), request
    assert request.name == "hpc"
    assert request.password == PASSWORD
    assert request.max_cores == 4
    assert request.max_mem == 14
    assert request.nfs_path == "/data/pulsar_storage/pulsar"


# Galaxy's runner and destination, from byoc-sync's templates


def galaxy_runner(amqp, form):
    names = byoc_names(amqp["name"])
    site = {**SITE["site"], "mq_host": amqp["host"], "mq_port": amqp["port"]}
    [runner] = yaml.safe_load(
        byoc_render("runner.yml.j2", names=names, site=site, req=form)
        # byoc-sync writes '{{ rabbitmq_password_... }}' for Ansible's vault to fill in.
        .replace("{{ " + names.vault_var + " }}", amqp["password"])
    )
    return runner


PARAMS = [
    pytest.param({}, id="defaults"),
    pytest.param(
        {
            "pulsar_storage_path": "/data/volume_2/pulsar",
            "pulsar_amqp_port": "15671",
            "pulsar_amqp_name": "eu_hpc_2",
            "pulsar_amqp_host": "mq.galaxyproject.eu",
        },
        id="custom-port-name-and-host",
    ),
]


@pytest.mark.parametrize("runner", ["local", "slurm"])
@pytest.mark.parametrize("params", PARAMS)
def test_app_yml_matches_byoc_syncs_runner(params, runner):
    params = {**params, "pulsar_job_runner": runner}
    ours = render_yaml("pulsar-app.yml.j2", **params)
    amqp = resolve("_pulsar_amqp", **params)
    theirs = galaxy_runner(amqp, resolve("_pulsar_byoc_form", **params))["params"]

    # Our TLS settings replace ?ssl=1, which would disable them (test_pulsar_compat).
    assert theirs["amqp_url"] == ours["message_queue_url"] + "?ssl=1"
    assert PASSWORD in ours["message_queue_url"]
    assert set(ours["managers"]) == {theirs["manager"]}
    assert theirs["manager"] == "production"
    assert ours["amqp_acknowledge"] is True
    assert theirs["amqp_acknowledge"] == "true"
    assert ours["amqp_ack_republish_time"] == theirs["amqp_ack_republish_time"] == 300
    assert ours["amqp_publish_retry"] is True
    assert theirs["amqp_publish_retry"] == "true"
    assert ours["amqp_publish_retry_max_retries"] == theirs["amqp_publish_retry_max_retries"]
    assert (
        ours["managers"]["production"]["type"]
        == {
            "local": "queued_python",
            "slurm": "queued_drmaa",
        }[runner]
    )


def test_destination_directories_are_ours():
    amqp = resolve("_pulsar_amqp")
    form = resolve("_pulsar_byoc_form")
    names = byoc_names(amqp["name"])
    req = {
        "max_cores": form["max_accepted_cores"],
        "max_mem": form["max_accepted_mem"],
        "min_gpus": 0,
        "max_gpus": 0,
        "nfs_path": form["nfs_path"],
    }
    text = byoc_render("destination.yml.j2", names=names, req=req, email="alice@example.org")
    [destination] = yaml.safe_load(text).values()
    app = render_yaml("pulsar-app.yml.j2")

    assert destination["params"]["jobs_directory"] == app["staging_directory"]
    assert destination["params"]["persistence_directory"] == app["persistence_directory"]
    assert destination["env"]["SINGULARITY_CACHEDIR"] == resolve("pulsar_container_cache_dir")
    assert destination["max_accepted_cores"] == 4
    assert destination["max_accepted_mem"] == 14


def test_mq_user_entry_is_ours():
    amqp = resolve("_pulsar_amqp")
    [entry] = yaml.safe_load(byoc_render("mq_user.yml.j2", names=byoc_names(amqp["name"])))

    assert entry["user"] == amqp["mq_user"]
    assert entry["vhost"] == amqp["vhost"]


def test_certificate_and_hostname_verification():
    ours = render_yaml("pulsar-app.yml.j2")

    assert ours["amqp_connect_ssl_cert_reqs"] == "cert_required"
    assert ours["amqp_connect_ssl_ca_certs"] == "/etc/ssl/certs/ca-certificates.crt"
    assert ours["amqp_connect_ssl_server_hostname"] == "mq.example.org"


def test_the_galaxy_component_contract_is_gone():
    assert not (BYOC_CONTRACT.parent / "galaxy_contract").exists()
    for path in ("README.md", "parameters.yml", "group_vars/all.yml", "playbook.yml"):
        assert "src_galaxy_pulsar_amqp_configs" not in (BYOC_CONTRACT.parents[1] / path).read_text()
    assert "pulsar_galaxy_fqdn" not in load_yaml("parameters.yml")


VENDORED_FILES = {
    "naming.py": "naming.py",
    "validate.py": "validate.py",
    "models.py": "models.py",
    "eu.yml": "sites/eu.yml",
    "templates/eu/runner.yml.j2": "templates/eu/runner.yml.j2",
    "templates/eu/destination.yml.j2": "templates/eu/destination.yml.j2",
    "templates/eu/mq_user.yml.j2": "templates/eu/mq_user.yml.j2",
}


@pytest.mark.skipif(
    not os.environ.get("BYOC_SYNC_REPO"),
    reason="set BYOC_SYNC_REPO to a checkout of the byoc-sync repository",
)
@pytest.mark.parametrize(("ours", "theirs"), VENDORED_FILES.items())
def test_vendored_files_match_the_byoc_sync_repository(ours, theirs):
    checkout = Path(os.environ["BYOC_SYNC_REPO"]) / "byoc-sync" / "src" / "byoc_sync"
    assert filecmp.cmp(BYOC_CONTRACT / ours, checkout / theirs, shallow=False), (
        f"{theirs} changed in the byoc-sync repository; "
        "review it and copy it to tests/byoc_contract/"
    )
