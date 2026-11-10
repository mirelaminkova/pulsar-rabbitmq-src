"""Component parameters: defaults, validation and the Galaxy entry they produce.

Validation must accept exactly what Galaxy's parser accepts for the same values
(vendored in tests/galaxy_contract/src_galaxy.py), so a deployment that passes
here cannot be rejected by the Galaxy component, and vice versa, apart from the
documented stricter rule for the Galaxy hostname. The storage path (and the
jobs_directory derived from it) is tested in tests/test_storage.py.
"""

import pytest

from filter_plugins.pulsar_amqp import pulsar_amqp_errors
from tests.galaxy_contract import src_galaxy
from tests.helpers import PASSWORD, resolve, settings


def galaxy_errors(entry_text):
    entries = src_galaxy.src_config_list(entry_text, "src_galaxy_pulsar_amqp_configs")
    return src_galaxy.pulsar_amqp_config_errors(entries)


# Defaults


def test_unset_parameters_get_galaxys_defaults():
    # SRC passes an unset parameter as an empty string.
    result = settings(port="", manager="")

    assert result["port"] == 5671
    assert result["manager"] == src_galaxy.AMQP_DEFAULTS["manager"]
    assert pulsar_amqp_errors(result) == []


def test_empty_optional_parameters_get_defaults():
    result = settings(
        port="",
        manager="",
        docker_enabled="",
        singularity_enabled="",
        cvmfs="",
    )

    assert pulsar_amqp_errors(result) == []
    assert result["docker_enabled"] is False
    assert result["singularity_enabled"] is True
    assert result["cvmfs"] is True


def test_derived_names_follow_galaxy():
    result = settings(name="eu-hpc")

    [config] = src_galaxy.pulsar_amqp_configs([{"name": "eu-hpc"}])
    assert result["vhost"] == config["vhost"]
    assert result["environment"] == config["environment"]


def test_url_has_no_ssl_query():
    # ?ssl=1 would replace Pulsar's amqp_connect_ssl_* settings (see test_pulsar_compat).
    assert settings()["url"] == f"pyamqp://hpc:{PASSWORD}@galaxy.example.org:5671//pulsar/hpc"


def test_values_are_trimmed():
    result = settings(galaxy_fqdn=" Galaxy.Example.org ", name=" hpc ")

    assert result["galaxy_fqdn"] == "galaxy.example.org"
    assert result["name"] == "hpc"


# Validation


def assert_invalid(message_part, **overrides):
    errors = pulsar_amqp_errors(settings(**overrides))
    assert any(message_part in error for error in errors), errors


def test_valid():
    assert pulsar_amqp_errors(settings()) == []


def test_required_values():
    errors = pulsar_amqp_errors(settings(galaxy_fqdn="", name="", password="", storage_path=""))

    assert len(errors) == 4, errors
    assert all("required" in error for error in errors), errors


@pytest.mark.parametrize(
    "value",
    [
        "https://galaxy.example.org",
        "galaxy.example.org/",
        "galaxy",
        "galaxy.example.org:5671",
        "-x.example.org",
        "a b.example.org",
    ],
)
def test_galaxy_fqdn_must_be_a_hostname(value):
    assert_invalid("pulsar_galaxy_fqdn", galaxy_fqdn=value)


@pytest.mark.parametrize(
    "value",
    # Galaxy also refuses the ports RabbitMQ, Nginx and Galaxy use themselves.
    ["0", "65536", "56x", "-1", "5671.0", "5672", "4369", "25672", "80", "443", "8080"],
)
def test_invalid_port(value):
    assert_invalid("pulsar_amqp_port", port=value)


def test_port():
    assert pulsar_amqp_errors(settings(port="15671")) == []


@pytest.mark.parametrize(
    "value",
    ["short", "has space in it", "with,comma1234", "quote'1234567890", "colon:1234567890"],
)
def test_password_is_never_echoed(value):
    errors = pulsar_amqp_errors(settings(password=value))
    assert len(errors) == 1, errors
    assert value not in " ".join(errors)


@pytest.mark.parametrize(
    ("key", "parameter"),
    [
        ("docker_enabled", "pulsar_docker"),
        ("singularity_enabled", "pulsar_apptainer"),
        ("cvmfs", "pulsar_cvmfs"),
    ],
)
def test_misspelt_booleans_are_errors(key, parameter):
    assert_invalid(parameter, **{key: "ture"})


def test_booleans():
    assert not settings(docker_enabled="false")["docker_enabled"]
    assert settings(docker_enabled=True)["docker_enabled"]
    assert_invalid("docker", docker_enabled="maybe")


# For name, password and manager: valid here <=> Galaxy accepts our entry.
SAME_RULES_AS_GALAXY = {
    "name": [
        "hpc",
        "eu_hpc-2",
        "a" * 64,
        "a" * 65,
        "HPC",
        "hp c",
        "galaxy",
        "guest",
        "true",
        "false",
        "hpc.1",
    ],
    "password": [
        PASSWORD,
        "abcdefghijkl",
        "abcdefghijk",
        "a~b.c_d-e123",
        "abc+defghijkl",
        "abc@defghijkl",
        "abc/defghijkl",
        "true" * 3,
        "x" * 200,
    ],
    "manager": ["_default_", "gpu", "Big-Queue_1", "a.b", "true", "with space"],
}


@pytest.mark.parametrize(
    ("key", "value"),
    [
        pytest.param(key, value, id=f"{key}-{value if len(value) <= 20 else f'{len(value)}chars'}")
        for key, values in SAME_RULES_AS_GALAXY.items()
        for value in values
    ],
)
def test_rules_match_galaxys_parser(key, value):
    result = settings(**{key: value})
    ours = [e for e in pulsar_amqp_errors(result) if key in e or "required" in e]
    theirs = galaxy_errors(result["galaxy_entry"])
    assert (ours == []) == (theirs == []), (ours, theirs)


# The Galaxy entry


def parsed(result):
    [entry] = src_galaxy.src_config_list(result["galaxy_entry"], "src_galaxy_pulsar_amqp_configs")
    return entry


def test_galaxy_parses_the_entry_without_errors():
    result = settings()

    assert galaxy_errors(result["galaxy_entry"]) == []
    assert set(parsed(result)) <= src_galaxy.AMQP_KEYS


def test_galaxy_derives_the_same_connection():
    result = settings(
        manager="gpu",
        storage_path="/data/volume_2/pulsar",
        docker_enabled="false",
        singularity_enabled="true",
    )
    [config] = src_galaxy.pulsar_amqp_configs([parsed(result)])

    assert config["name"] == "hpc"
    assert config["password"] == PASSWORD
    assert config["vhost"] == result["vhost"]
    assert config["manager"] == "gpu"
    assert config["jobs_directory"] == "/data/volume_2/pulsar/staging"
    assert config["singularity_enabled"] is True
    assert config["docker_enabled"] is False


# With both, Galaxy would prefer Docker; Apptainer runs unprivileged.
@pytest.mark.parametrize(
    ("docker", "singularity", "expected"),
    [
        ("true", "true", {"singularity_enabled"}),
        ("true", "false", {"docker_enabled"}),
        ("false", "true", {"singularity_enabled"}),
        ("false", "false", set()),
    ],
    ids=["both-apptainer-wins", "docker", "apptainer", "none"],
)
def test_one_container_type(docker, singularity, expected):
    entry = parsed(settings(docker_enabled=docker, singularity_enabled=singularity))
    assert {key for key in entry if key.endswith("_enabled")} == expected


def test_one_line():
    assert "\n" not in settings()["galaxy_entry"]


# group_vars


def test_group_vars_build_the_settings_from_the_component_parameters():
    result = resolve("_pulsar_amqp")

    assert result["url"] == f"pyamqp://hpc:{PASSWORD}@galaxy.example.org:5671//pulsar/hpc"
    assert result["jobs_directory"] == "/data/pulsar_storage/pulsar/staging"
    assert resolve("_pulsar_amqp_errors") == []


def test_group_vars_report_missing_parameters():
    errors = resolve("_pulsar_amqp_errors", pulsar_galaxy_fqdn="", pulsar_amqp_password="")

    assert len(errors) == 2, errors
