"""Connection settings for a Pulsar that consumes jobs from the Galaxy component's RabbitMQ.

Loaded automatically by Ansible because this directory sits next to playbook.yml.

The Galaxy side (mirelaminkova/src-component-galaxy-amqp) is the contract: for each
``src_galaxy_pulsar_amqp_configs`` entry ``<name>`` it creates RabbitMQ user ``<name>``,
vhost ``/pulsar/<name>`` and environment ``amqp_<name>``. The rules below are those of
its parser (``filter_plugins/src_galaxy.py``); tests/test_parameters.py checks that both
accept the same values.
"""

import re

DEFAULTS = {
    "port": 5671,  # src_galaxy_rabbitmq_port
    "manager": "_default_",
    "jobs_directory": "/mnt/pulsar/files/staging",
}

# As in Galaxy's parser.
_NAME = re.compile(r"[a-z0-9_-]+")
_MANAGER = re.compile(r"[A-Za-z0-9_-]+")
_PASSWORD = re.compile(r"[A-Za-z0-9._~-]{12,}")
_RESERVED_NAMES = ("galaxy", "guest")
# Galaxy's one-line parser turns these bare values into booleans, which it then rejects.
_BOOLEAN_WORDS = ("true", "false")
# Stricter than Galaxy's `/\S*`: Pulsar owns this directory, and the value must survive
# the one-line SRC format (no `,` `[` `]` `{` `}` or quotes).
_JOBS_DIRECTORY = re.compile(r"(/[A-Za-z0-9_.-]+){2,}")
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_FQDN = re.compile(rf"(?=.{{1,253}}$){_LABEL}(?:\.{_LABEL})+")
_BOOLEANS = {True: True, False: False, "true": True, "false": False}


def _text(value):
    return "" if value is None else str(value).strip()


def _boolean(value):
    if isinstance(value, bool):
        return value
    return _BOOLEANS.get(_text(value).lower(), value)


def pulsar_amqp_settings(params):
    """Normalise the component parameters and derive the names Galaxy and Pulsar use.

    ``params`` has the keys galaxy_fqdn, port, name, password, manager, jobs_directory,
    docker_enabled and singularity_enabled. Empty values (how SRC passes an unset
    parameter) get Galaxy's defaults. The result is meaningful only without
    :func:`pulsar_amqp_errors`.
    """
    port = _text(params.get("port")) or str(DEFAULTS["port"])
    settings = {
        "galaxy_fqdn": _text(params.get("galaxy_fqdn")).lower(),
        "port": int(port) if port.isdigit() else port,
        "name": _text(params.get("name")),
        "password": _text(params.get("password")),
        "manager": _text(params.get("manager")) or DEFAULTS["manager"],
        "jobs_directory": _text(params.get("jobs_directory")) or DEFAULTS["jobs_directory"],
        "docker_enabled": _boolean(params.get("docker_enabled", False)),
        "singularity_enabled": _boolean(params.get("singularity_enabled", False)),
    }
    name, manager = settings["name"], settings["manager"]
    settings.update(
        vhost=f"/pulsar/{name}",
        environment=f"amqp_{name}",
        tag=f"{name}-pulsar",
        # Pulsar's queue for new jobs: "<key prefix>_setup", prefix "pulsar_" for the
        # default manager and "pulsar_<manager>_" otherwise.
        setup_queue="pulsar__setup" if manager == "_default_" else f"pulsar_{manager}__setup",
        # No `?ssl=1`: kombu would let it replace the amqp_connect_ssl_* settings that
        # verify the certificate and hostname; those settings switch TLS on by themselves.
        url=(
            f"pyamqp://{name}:{settings['password']}@{settings['galaxy_fqdn']}:{settings['port']}"
            f"//pulsar/{name}"
        ),
    )
    settings["galaxy_entry"] = _galaxy_entry(settings)
    return settings


def _galaxy_entry(settings):
    """The ``src_galaxy_pulsar_amqp_configs`` value for this Pulsar, in SRC's one-line format."""
    values = {key: settings[key] for key in ("name", "password", "manager", "jobs_directory")}
    # One container type: with both, Galaxy would prefer Docker, while Apptainer uses the
    # CVMFS images and the system-wide /cvmfs bind. Docker stays usable by hand-edited
    # Galaxy configurations.
    if settings["singularity_enabled"] is True:
        values["singularity_enabled"] = "true"
    elif settings["docker_enabled"] is True:
        values["docker_enabled"] = "true"
    return "[{" + ", ".join(f"{key}: {value}" for key, value in values.items()) + "}]"


def pulsar_amqp_errors(settings):
    """Return one message per invalid parameter; values of secrets are never included."""
    errors = []
    fqdn = settings["galaxy_fqdn"]
    if not fqdn:
        errors.append("pulsar_galaxy_fqdn is required: the Galaxy workspace's hostname")
    elif not _FQDN.fullmatch(fqdn):
        errors.append(
            f"pulsar_galaxy_fqdn must be a hostname such as galaxy.example.org "
            f"(no https:// or port), got '{fqdn}'"
        )

    port = settings["port"]
    if not (isinstance(port, int) and 1 <= port <= 65535):
        errors.append(
            "pulsar_amqp_port must be an integer from 1 to 65535 (src_galaxy_rabbitmq_port), "
            f"got '{port}'"
        )

    name = settings["name"]
    if not name:
        errors.append(
            "pulsar_amqp_name is required: "
            "the name of this Pulsar in src_galaxy_pulsar_amqp_configs"
        )
    elif not _NAME.fullmatch(name) or len(name) > 64 or name in _RESERVED_NAMES + _BOOLEAN_WORDS:
        errors.append(
            "pulsar_amqp_name must be at most 64 of a-z 0-9 _ - "
            "and not galaxy, guest, true or false, "
            f"got '{name}'"
        )

    if not settings["password"]:
        errors.append(
            "pulsar_amqp_password is required: the password of this Pulsar's entry in "
            "src_galaxy_pulsar_amqp_configs (or /etc/galaxy-rabbitmq/pulsar_<name>.txt on Galaxy)"
        )
    elif not _PASSWORD.fullmatch(settings["password"]):
        errors.append(
            "pulsar_amqp_password must be at least 12 characters from A-Z a-z 0-9 . _ ~ -"
        )

    manager = settings["manager"]
    if not _MANAGER.fullmatch(manager) or manager in _BOOLEAN_WORDS:
        errors.append(
            f"pulsar_manager must match [A-Za-z0-9_-]+ and not be true or false, got '{manager}'"
        )

    jobs_directory = settings["jobs_directory"]
    if (
        not _JOBS_DIRECTORY.fullmatch(jobs_directory)
        or "/../" in jobs_directory + "/"
        or "/./" in jobs_directory + "/"
    ):
        errors.append(
            "pulsar_staging_dir must be an absolute path of at least two levels of "
            f"A-Z a-z 0-9 _ . - without . or .. (Galaxy's jobs_directory), got '{jobs_directory}'"
        )

    for key, parameter in (
        ("docker_enabled", "pulsar_docker"),
        ("singularity_enabled", "pulsar_apptainer"),
    ):
        if not isinstance(settings[key], bool):
            errors.append(f"{parameter} must be true or false, got '{settings[key]}'")
    return errors


class FilterModule:
    def filters(self):
        return {
            "pulsar_amqp_settings": pulsar_amqp_settings,
            "pulsar_amqp_errors": pulsar_amqp_errors,
        }
