"""Component parameters: defaults, validation and the BYOC names they produce.

The BYOC name and password are validated with byoc-sync's rules (vendored in
tests/byoc_contract/validate.py; tests/test_byoc_contract.py checks that both accept the same
values), so a deployment that passes here cannot be rejected by the byoc-sync pipeline. The
storage path (and the jobs_directory derived from it) is tested in tests/test_storage.py.
"""

import pytest

from filter_plugins.pulsar_amqp import pulsar_amqp_errors, pulsar_byoc_form
from tests.helpers import MQ_USER, PASSWORD, URL, VHOST, resolve, settings

# Defaults


def test_unset_parameters_get_usegalaxy_eus_defaults():
    # SRC passes an unset parameter as an empty string.
    result = settings(host="", port="", manager="", ack_republish_time="")

    assert result["host"] == "mq.galaxyproject.eu"
    assert result["port"] == 5671
    assert result["manager"] == "production"
    assert result["ack_republish_time"] == 300
    assert pulsar_amqp_errors(result) == []


def test_empty_optional_parameters_get_defaults():
    result = settings(docker_enabled="", singularity_enabled="", cvmfs="")

    assert pulsar_amqp_errors(result) == []
    assert result["docker_enabled"] is False
    assert result["singularity_enabled"] is True
    assert result["cvmfs"] is True


# The names byoc-sync derives from the BYOC name


def test_derived_names_follow_byoc_sync():
    result = settings(name="alice01")

    assert result["mq_user"] == "galaxy_alice01_byoc"
    assert result["vhost"] == "/pulsar/galaxy_alice01_byoc"
    assert result["setup_queue"] == "pulsar_production__setup"


def test_url_has_no_ssl_query():
    # ?ssl=1 would replace Pulsar's amqp_connect_ssl_* settings (see test_pulsar_compat).
    assert settings()["url"] == URL
    assert "ssl" not in settings()["url"]


def test_values_are_trimmed_and_the_host_lowercased():
    result = settings(host=" MQ.GalaxyProject.eu ", name=" hpc ")

    assert result["host"] == "mq.galaxyproject.eu"
    assert result["name"] == "hpc"
    assert result["url"] == f"pyamqp://{MQ_USER}:{PASSWORD}@mq.galaxyproject.eu:5671/{VHOST}"


def test_the_old_entry_mode_is_gone():
    result = settings()

    assert "galaxy_fqdn" not in result
    assert "galaxy_entry" not in result
    assert "environment" not in result


# Generated credentials: an empty name or password is not an error here (tasks/credentials.yml
# generates or loads them after the storage is ready).


@pytest.mark.parametrize("key", ["name", "password"])
@pytest.mark.parametrize("value", ["", "  ", None])
def test_an_empty_name_or_password_is_generated_later(key, value):
    result = settings(**{key: value})

    assert result[key] == ""
    assert pulsar_amqp_errors(result) == []


# The broker host


@pytest.mark.parametrize(
    "value",
    [
        "https://mq.galaxyproject.eu",
        "mq.galaxyproject.eu/",
        "mq",
        "mq.galaxyproject.eu:5671",
        "-x.galaxyproject.eu",
        "a b.galaxyproject.eu",
    ],
)
def test_broker_host_must_be_a_hostname(value):
    errors = pulsar_amqp_errors(settings(host=value))

    assert len(errors) == 1, errors
    assert "pulsar_amqp_host" in errors[0]
    assert PASSWORD.lower() not in errors[0].lower()


# The README shows usegalaxy.eu's runner URL; pasted into a field, it holds the password.
RUNNER_URL = f"pyamqp://{MQ_USER}:{PASSWORD}@mq.galaxyproject.eu:5671/{VHOST}?ssl=1"


@pytest.mark.parametrize(
    "value",
    [
        RUNNER_URL,
        RUNNER_URL.replace("@", "%40"),
        f"{MQ_USER}:{PASSWORD}",
        PASSWORD,  # the password in the wrong field
        f"mq.galaxyproject.eu\n{PASSWORD}",
    ],
    ids=["url", "url-percent-encoded", "login", "password", "newline"],
)
@pytest.mark.parametrize("key", ["host", "port", "name", "storage_path", "job_runner", "cvmfs"])
def test_a_pasted_password_is_never_echoed(key, value):
    errors = pulsar_amqp_errors(settings(**{key: value}))

    assert len(errors) == 1, errors
    assert "the password" in errors[0]
    assert PASSWORD.lower() not in errors[0].lower()


@pytest.mark.parametrize("value", [RUNNER_URL, f"{MQ_USER}:{PASSWORD}"], ids=["url", "login"])
def test_a_pasted_url_is_never_echoed_as_the_manager(value):
    # A bare password is a valid manager name (letters and digits), so only these fail.
    errors = pulsar_amqp_errors(settings(manager=value))

    assert len(errors) == 1, errors
    assert "the password" in errors[0]
    assert PASSWORD.lower() not in errors[0].lower()


def test_a_pasted_url_is_never_echoed_as_the_republish_time():
    errors = pulsar_amqp_errors(settings(ack_republish_time=RUNNER_URL))

    assert len(errors) == 1, errors
    assert PASSWORD.lower() not in errors[0].lower()
    assert MQ_USER not in errors[0]


@pytest.mark.parametrize("key", ["port", "ack_republish_time"])
def test_a_huge_number_is_an_error_not_a_crash(key):
    # int() refuses more than 4300 digits (Python 3.11+); the filter must not.
    errors = pulsar_amqp_errors(settings(**{key: "9" * 5000}))

    assert len(errors) == 1, errors


# amqp_ack_republish_time: 300 s in byoc-sync's runner.


@pytest.mark.parametrize("value", ["", None, "300"])
def test_ack_republish_time_defaults_to_byoc_syncs(value):
    result = settings(ack_republish_time=value)

    assert result["ack_republish_time"] == 300
    assert pulsar_amqp_errors(result) == []


def test_ack_republish_time_can_be_changed():
    result = settings(ack_republish_time=" 1200 ")

    assert result["ack_republish_time"] == 1200
    assert pulsar_amqp_errors(result) == []


@pytest.mark.parametrize(
    "value", ["0", "-300", "300.5", "5m", "1e3", "\u00b2", "\uff13\uff10\uff10"]
)
def test_ack_republish_time_must_be_a_positive_number_of_seconds(value):
    errors = pulsar_amqp_errors(settings(ack_republish_time=value))

    assert len(errors) == 1, errors
    assert "pulsar_amqp_ack_republish_time" in errors[0]
    assert value in errors[0]


# Validation


def assert_invalid(message_part, **overrides):
    errors = pulsar_amqp_errors(settings(**overrides))
    assert any(message_part in error for error in errors), errors


def test_valid():
    assert pulsar_amqp_errors(settings()) == []


def test_required_values():
    errors = pulsar_amqp_errors(settings(host="", storage_path=""))

    assert len(errors) == 1, errors
    assert "pulsar_storage_path is required" in errors[0]


@pytest.mark.parametrize(
    "value",
    # Galaxy's broker port: any port, but a port.
    ["0", "65536", "56x", "-1", "5671.0"],
)
def test_invalid_port(value):
    assert_invalid("pulsar_amqp_port", port=value)


@pytest.mark.parametrize("value", ["15671", "5672"])
def test_port(value):
    assert pulsar_amqp_errors(settings(port=value)) == []


@pytest.mark.parametrize("value", ["\u00b2", "\uff15\uff16\uff17\uff11"])
def test_only_ascii_digits_make_a_port(value):
    # str.isdigit() accepts a superscript, which int() refuses, and fullwidth digits.
    assert_invalid("pulsar_amqp_port", port=value)


# byoc-sync's rules (validate.py): NAME_RE, no trailing _, no _byoc suffix, no __.
VALID_NAMES = ["hpc", "alice01", "eu_hpc_2", "abc", "a" * 32, "srcdeadbeef"]
INVALID_NAMES = [
    "ab",  # too short
    "a" * 33,
    "HPC",
    "1abc",
    "_abc",
    "hp c",
    "eu-hpc",
    "hpc.1",
    "hpc_",  # ends with _
    "a1_",
    "hpc_byoc",  # the suffix byoc-sync adds
    "hp__c",  # a double underscore
    "galaxy_hpc_byoc",  # the RabbitMQ user, not the name
]


@pytest.mark.parametrize("value", VALID_NAMES)
def test_valid_names(value):
    assert pulsar_amqp_errors(settings(name=value)) == []


@pytest.mark.parametrize("value", INVALID_NAMES)
def test_invalid_names_are_refused(value):
    errors = pulsar_amqp_errors(settings(name=value))

    assert len(errors) == 1, errors
    assert "pulsar_amqp_name" in errors[0]
    assert value in errors[0]


# byoc-sync's rule (validate.py): PASSWORD_RE, 10 to 128 letters and digits.
VALID_PASSWORDS = [PASSWORD, "a" * 10, "A1b2C3d4E5", "z" * 128]
INVALID_PASSWORDS = [
    "short",
    "a" * 9,
    "a" * 129,
    "has space in it",
    "with,comma1234",
    "quote'1234567890",
    "colon:1234567890",
    "Secret.Pass_word~123",  # punctuation
    "abc@defghijkl",
]


@pytest.mark.parametrize("value", VALID_PASSWORDS)
def test_valid_passwords(value):
    assert pulsar_amqp_errors(settings(password=value)) == []


@pytest.mark.parametrize("value", INVALID_PASSWORDS)
def test_invalid_passwords_are_refused_and_never_echoed(value):
    errors = pulsar_amqp_errors(settings(password=value))

    assert len(errors) == 1, errors
    assert "pulsar_amqp_password" in errors[0]
    assert value not in " ".join(errors)
    assert "letters and digits" in errors[0]


@pytest.mark.parametrize("value", ["gpu", "Big-Queue_1", "_default_"])
def test_manager_names(value):
    result = settings(manager=value)

    assert result["manager"] == value
    assert pulsar_amqp_errors(result) == []


@pytest.mark.parametrize("value", ["a.b", "true", "with space"])
def test_invalid_manager_names(value):
    assert_invalid("pulsar_manager", manager=value)


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


# The BYOC form (the welcome page shows it; byoc-sync reads it from Galaxy's preferences)


def test_byoc_form_fields():
    form = pulsar_byoc_form(settings(), 8, 32000)

    assert form == {
        "username": "hpc",
        "max_accepted_cores": 8,
        # int(0.9 * memory MB / 1024), as the pulsar-byoc playbook's connection summary.
        "max_accepted_mem": 28,
        "min_accepted_gpus": 0,
        "max_accepted_gpus": 0,
        "nfs_path": "/data/pulsar_storage/pulsar",
    }
    assert PASSWORD not in str(form)


@pytest.mark.parametrize(
    ("cpus", "memory_mb", "cores", "mem"),
    [
        (1, 1024, 1, 1),  # a tiny workspace: byoc-sync needs at least 0.1 GB
        (4, 16000, 4, 14),
        (" 4 ", "16000", 4, 14),  # facts as strings
        (64, 262144, 64, 230),
    ],
)
def test_byoc_form_limits_follow_the_workspace(cpus, memory_mb, cores, mem):
    form = pulsar_byoc_form(settings(), cpus, memory_mb)

    assert (form["max_accepted_cores"], form["max_accepted_mem"]) == (cores, mem)


# group_vars


def test_group_vars_build_the_settings_from_the_component_parameters():
    result = resolve("_pulsar_amqp")

    assert result["url"] == URL
    assert result["mq_user"] == MQ_USER
    assert result["vhost"] == VHOST
    assert result["jobs_directory"] == "/data/pulsar_storage/pulsar/staging"
    assert resolve("_pulsar_amqp_errors") == []


def test_group_vars_defaults_are_usegalaxy_eus():
    result = resolve(
        "_pulsar_amqp", pulsar_amqp_host="", pulsar_manager="", pulsar_amqp_ack_republish_time=""
    )

    assert result["host"] == "mq.galaxyproject.eu"
    assert result["url"] == f"pyamqp://{MQ_USER}:{PASSWORD}@mq.galaxyproject.eu:5671/{VHOST}"
    assert result["ack_republish_time"] == 300
    assert result["manager"] == "production"


def test_group_vars_report_an_invalid_broker_host():
    errors = resolve("_pulsar_amqp_errors", pulsar_amqp_host="https://mq.galaxyproject.eu")

    assert len(errors) == 1, errors
    assert "pulsar_amqp_host" in errors[0]


def test_group_vars_report_invalid_credentials():
    errors = resolve(
        "_pulsar_amqp_errors",
        pulsar_amqp_name="hpc_byoc",
        pulsar_amqp_password="short",  # noqa: S106 - an invalid test value
    )

    assert len(errors) == 2, errors
    assert "short" not in " ".join(errors)


def test_group_vars_keep_the_raw_parameters_for_the_credentials_task():
    # tasks/credentials.yml rebuilds _pulsar_amqp from these with the resolved name and password.
    params = resolve("_pulsar_amqp_params")

    assert params["name"] == "hpc"
    assert params["password"] == PASSWORD
    assert resolve("_pulsar_amqp", _pulsar_amqp_params={**params, "name": "other"})["mq_user"] == (
        "galaxy_other_byoc"
    )


def test_group_vars_build_the_byoc_form_from_the_facts():
    form = resolve("_pulsar_byoc_form")

    assert form["username"] == "hpc"
    assert form["max_accepted_cores"] == 4
    assert form["max_accepted_mem"] == 14
    assert form["nfs_path"] == "/data/pulsar_storage/pulsar"
