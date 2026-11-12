"""Connection settings for a Pulsar that consumes jobs from the Galaxy component's RabbitMQ.

Loaded automatically by Ansible because this directory sits next to playbook.yml.

The Galaxy side (mirelaminkova/src-component-galaxy-amqp) is the contract: for each
``src_galaxy_pulsar_amqp_configs`` entry ``<name>`` it creates RabbitMQ user ``<name>``,
vhost ``/pulsar/<name>`` and environment ``amqp_<name>``. The rules below are those of
its parser (``filter_plugins/src_galaxy.py``); tests/test_parameters.py checks that both
accept the same values, apart from the stricter hostname rule. Galaxy's ``jobs_directory``
is not a parameter here: it is derived from the required storage path, so the two
cannot disagree.
"""

import os
import re

DEFAULTS = {
    "port": 5671,  # src_galaxy_rabbitmq_port
    # Galaxy's runner (job_conf.yml.j2) and Pulsar republish an unacknowledged message
    # after this many seconds; usegalaxy.eu's runners use 300.
    "ack_republish_time": 1200,
    "manager": "_default_",
    "docker_enabled": False,
    "singularity_enabled": True,
    "cvmfs": True,
    "job_runner": "slurm",
}
# pulsar_job_runner: Slurm on this workspace through DRMAA, or Pulsar itself.
_JOB_RUNNERS = ("slurm", "local")

# As in Galaxy's parser.
_NAME = re.compile(r"[a-z0-9_-]+")
_MANAGER = re.compile(r"[A-Za-z0-9_-]+")
_PASSWORD = re.compile(r"[A-Za-z0-9._~-]{12,}")
_RESERVED_NAMES = ("galaxy", "guest")
# Galaxy's one-line parser turns these bare values into booleans, which it then rejects.
_BOOLEAN_WORDS = ("true", "false")
# A directory on an attached volume, which SRC mounts at /data/<volume name>: below the
# mount point (the playbook makes the storage directory root's), never a system
# directory or Pulsar's installation. Galaxy's jobs_directory (`/\S*` there) is
# <storage>/staging, so the value must also survive the one-line SRC format.
_STORAGE_PATH = re.compile(r"/data(?:/[A-Za-z0-9_.-]+){2,}")
# The mount point itself, as users tend to enter it: its "pulsar" directory is used.
_MOUNT_POINT = re.compile(r"/data/([A-Za-z0-9_.-]+)/?")
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_FQDN = re.compile(rf"(?=.{{1,253}}$){_LABEL}(?:\.{_LABEL})+")
# Galaxy's _galaxy_rabbitmq_reserved_ports: RabbitMQ's own, Nginx's and Galaxy's ports.
_RESERVED_PORTS = (5672, 4369, 25672, 80, 443, 8080)
_BOOLEANS = {True: True, False: False, "true": True, "false": False}
# ASCII digits only: str.isdigit() also accepts superscripts, which int() refuses. Ten
# digits at most: int() refuses thousands, and nothing longer is a port or a time.
_INTEGER = re.compile(r"[0-9]{1,10}")


def _text(value):
    return "" if value is None else str(value).strip()


def _integer(value, default):
    text = _text(value) or str(default)
    return int(text) if _INTEGER.fullmatch(text) else text


def _shown(value, password):
    """``value`` for an error message, unless it holds the password: a pasted runner URL,
    or the password in the wrong field. Hostnames are lowercased, hence the fold."""
    if "@" in value or (password and password.lower() in value.lower()):
        return "a value with a login or the password"
    return value


def _boolean(value, default):
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if not text:
        return default
    return _BOOLEANS.get(text, value)


def _storage_directory(value):
    """``/data/<volume name>`` (the volume's mount point) becomes ``/data/<volume name>/pulsar``."""
    mount_point = _MOUNT_POINT.fullmatch(value)
    if mount_point and mount_point[1] not in (".", ".."):
        return f"/data/{mount_point[1]}/pulsar"
    return value


def pulsar_amqp_settings(params):
    """Normalise the component parameters and derive the names Galaxy and Pulsar use.

    ``params`` has the keys galaxy_fqdn, host, port, name, password, manager, storage_path,
    docker_enabled, singularity_enabled, cvmfs, job_runner and ack_republish_time. Empty
    values (how SRC passes an unset parameter) get Galaxy's defaults, or this component's
    for cvmfs and job_runner. The broker ``host`` is the Galaxy host unless set: the Galaxy
    component runs RabbitMQ on the Galaxy workspace, usegalaxy.eu's Pulsar Network on a host
    of its own (mq.galaxyproject.eu). A volume's mount point as storage_path stands for its
    ``pulsar`` directory. The result is meaningful only without :func:`pulsar_amqp_errors`.
    """
    galaxy_fqdn = _text(params.get("galaxy_fqdn")).lower()
    settings = {
        "galaxy_fqdn": galaxy_fqdn,
        # The RabbitMQ host: the URL's host and the name its certificate is checked for.
        "host": _text(params.get("host")).lower() or galaxy_fqdn,
        "port": _integer(params.get("port"), DEFAULTS["port"]),
        "ack_republish_time": _integer(
            params.get("ack_republish_time"), DEFAULTS["ack_republish_time"]
        ),
        "name": _text(params.get("name")),
        "password": _text(params.get("password")),
        "manager": _text(params.get("manager")) or DEFAULTS["manager"],
        "storage_path": _storage_directory(_text(params.get("storage_path"))),
        # Only this workspace sees the runner; Galaxy's entry stays the same.
        "job_runner": _text(params.get("job_runner")).lower() or DEFAULTS["job_runner"],
    }
    # Galaxy's jobs_directory is Pulsar's staging directory, always on the storage.
    settings["jobs_directory"] = f"{settings['storage_path']}/staging"
    for key in ("docker_enabled", "singularity_enabled", "cvmfs"):
        settings[key] = _boolean(params.get(key), DEFAULTS[key])
    name, manager = settings["name"], settings["manager"]
    settings.update(
        vhost=f"/pulsar/{name}",
        environment=f"amqp_{name}",
        # Pulsar's queue for new jobs: "<key prefix>_setup", prefix "pulsar_" for the
        # default manager and "pulsar_<manager>_" otherwise.
        setup_queue="pulsar__setup" if manager == "_default_" else f"pulsar_{manager}__setup",
        # No `?ssl=1`: kombu would let it replace the amqp_connect_ssl_* settings that
        # verify the certificate and hostname; those settings switch TLS on by themselves.
        url=(
            f"pyamqp://{name}:{settings['password']}@{settings['host']}:{settings['port']}"
            f"//pulsar/{name}"
        ),
    )
    settings["galaxy_entry"] = _galaxy_entry(settings)
    return settings


def _galaxy_entry(settings):
    """The ``src_galaxy_pulsar_amqp_configs`` value for this Pulsar, in SRC's one-line format."""
    values = {key: settings[key] for key in ("name", "password", "manager", "jobs_directory")}
    # One container type: with both, Galaxy would prefer Docker, while Apptainer runs
    # unprivileged and sees /cvmfs through the system-wide bind.
    if settings["singularity_enabled"] is True:
        values["singularity_enabled"] = "true"
    elif settings["docker_enabled"] is True:
        values["docker_enabled"] = "true"
    return "[{" + ", ".join(f"{key}: {value}" for key, value in values.items()) + "}]"


def pulsar_amqp_errors(settings):
    """Return one message per invalid parameter; values of secrets are never included."""
    errors = []

    def shown(value):
        return _shown(str(value), settings["password"])

    fqdn = settings["galaxy_fqdn"]
    if not fqdn:
        errors.append("pulsar_galaxy_fqdn is required: the Galaxy workspace's hostname")
    elif not _FQDN.fullmatch(fqdn):
        errors.append(
            f"pulsar_galaxy_fqdn must be a hostname such as galaxy.example.org "
            f"(no https:// or port), got '{shown(fqdn)}'"
        )

    # A host equal to the Galaxy host (empty pulsar_amqp_host) is validated above as
    # pulsar_galaxy_fqdn.
    host = settings["host"]
    if host != fqdn and not _FQDN.fullmatch(host):
        errors.append(
            f"pulsar_amqp_host must be the RabbitMQ hostname such as mq.galaxyproject.eu "
            f"(no https:// or port), or empty for the Galaxy host, got '{shown(host)}'"
        )

    port = settings["port"]
    if not (isinstance(port, int) and 1 <= port <= 65535) or port in _RESERVED_PORTS:
        errors.append(
            "pulsar_amqp_port must be src_galaxy_rabbitmq_port: an integer from 1 to 65535, "
            f"not {', '.join(map(str, _RESERVED_PORTS))}, "
            f"got '{shown(port)}'"
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
            f"got '{shown(name)}'"
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

    republish = settings["ack_republish_time"]
    if not (isinstance(republish, int) and republish >= 1):
        errors.append(
            "pulsar_amqp_ack_republish_time must be a whole number of seconds, at least 1 "
            "(1200 for the Galaxy component's runner, 300 for usegalaxy.eu's), "
            f"got '{shown(republish)}'"
        )

    manager = settings["manager"]
    if not _MANAGER.fullmatch(manager) or manager in _BOOLEAN_WORDS:
        errors.append(
            "pulsar_manager must match [A-Za-z0-9_-]+ and not be true or false, "
            f"got '{shown(manager)}'"
        )

    storage_path = settings["storage_path"]
    if not storage_path:
        errors.append(
            "pulsar_storage_path is required: attach a storage volume to the workspace and "
            "set it to the volume's mount point, /data/<volume name> (Pulsar then uses "
            "/data/<volume name>/pulsar), or to a directory on the volume"
        )
    elif (
        not _STORAGE_PATH.fullmatch(storage_path) or os.path.normpath(storage_path) != storage_path
    ):
        errors.append(
            "pulsar_storage_path must be an attached storage volume's mount point, "
            "/data/<volume name>, or a directory on it, /data/<volume name>/<directory>, "
            "of A-Z a-z 0-9 _ . - without . or .., "
            f"got '{shown(storage_path)}'"
        )

    for key, parameter in (
        ("docker_enabled", "pulsar_docker"),
        ("singularity_enabled", "pulsar_apptainer"),
        ("cvmfs", "pulsar_cvmfs"),
    ):
        if not isinstance(settings[key], bool):
            errors.append(f"{parameter} must be true or false, got '{shown(settings[key])}'")

    if settings["job_runner"] not in _JOB_RUNNERS:
        errors.append(
            f"pulsar_job_runner must be slurm or local, got '{shown(settings['job_runner'])}'"
        )
    return errors


class FilterModule:
    def filters(self):
        return {
            "pulsar_amqp_settings": pulsar_amqp_settings,
            "pulsar_amqp_errors": pulsar_amqp_errors,
        }
