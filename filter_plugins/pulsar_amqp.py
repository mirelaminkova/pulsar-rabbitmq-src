"""Connection settings for a BYOC Pulsar that consumes jobs from a Galaxy's RabbitMQ.

Loaded automatically by Ansible because this directory sits next to playbook.yml.

The byoc-sync pipeline (sanjaysrikakulam/elixir_biohackathon_2026_project1, byoc-sync/) is
the contract: the user enters this Pulsar's BYOC name and password in the ``byoc_pulsar``
section of their Galaxy preferences, and byoc-sync creates RabbitMQ user
``galaxy_<name>_byoc``, vhost ``/pulsar/galaxy_<name>_byoc`` and a runner with manager
``production`` for it (its ``naming.py``). The rules below for the name and the password are
those of its ``validate.py``; tests/test_byoc_contract.py checks that both accept the same
values. The form's ``nfs_path`` is the storage directory, so Galaxy's ``jobs_directory``
(``<nfs_path>/staging``) cannot disagree with Pulsar's staging directory.
"""

import os
import re

DEFAULTS = {
    # usegalaxy.eu's broker (byoc-sync's sites/eu.yml).
    "host": "mq.galaxyproject.eu",
    "port": 5671,
    # byoc-sync's runner: Galaxy and Pulsar republish an unacknowledged message after this
    # many seconds.
    "ack_republish_time": 300,
    "manager": "production",
    "docker_enabled": False,
    "singularity_enabled": True,
    "cvmfs": True,
    "job_runner": "slurm",
}
# pulsar_job_runner: Slurm on this workspace through DRMAA, or Pulsar itself.
_JOB_RUNNERS = ("slurm", "local")

# byoc-sync's validate.py: NAME_RE and PASSWORD_RE, and the name's extra rules.
_NAME = re.compile(r"[a-z][a-z0-9_]{2,31}")
_PASSWORD = re.compile(r"[A-Za-z0-9]{10,128}")
# byoc-sync's nfs_path (the storage directory) is at most this long.
_NFS_PATH_MAX = 255
_MANAGER = re.compile(r"[A-Za-z0-9_-]+")
# YAML would read these bare values as booleans.
_BOOLEAN_WORDS = ("true", "false")
# A directory on an attached volume, which SRC mounts at /data/<volume name>: below the
# mount point (the playbook makes the storage directory root's), never a system
# directory or Pulsar's installation. Stricter than byoc-sync's nfs_path rule
# (^/[A-Za-z0-9._/-]*$, no // and no . or .. segments), so every accepted value passes it.
_STORAGE_PATH = re.compile(r"/data(?:/[A-Za-z0-9_.-]+){2,}")
# The mount point itself, as users tend to enter it: its "pulsar" directory is used.
_MOUNT_POINT = re.compile(r"/data/([A-Za-z0-9_.-]+)/?")
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_FQDN = re.compile(rf"(?=.{{1,253}}$){_LABEL}(?:\.{_LABEL})+")
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
    """Normalise the component parameters and derive the names byoc-sync and Pulsar use.

    ``params`` has the keys host, port, name, password, manager, storage_path,
    docker_enabled, singularity_enabled, cvmfs, job_runner and ack_republish_time. Empty
    values (how SRC passes an unset parameter) get usegalaxy.eu's defaults, or this
    component's for cvmfs and job_runner. An empty ``name`` or ``password`` stays empty:
    tasks/credentials.yml generates or loads them and calls this again. A volume's mount
    point as storage_path stands for its ``pulsar`` directory. The result is meaningful only
    without :func:`pulsar_amqp_errors`.
    """
    settings = {
        # The RabbitMQ host: the URL's host and the name its certificate is checked for.
        "host": _text(params.get("host")).lower() or DEFAULTS["host"],
        "port": _integer(params.get("port"), DEFAULTS["port"]),
        "ack_republish_time": _integer(
            params.get("ack_republish_time"), DEFAULTS["ack_republish_time"]
        ),
        "name": _text(params.get("name")),
        "password": _text(params.get("password")),
        "manager": _text(params.get("manager")) or DEFAULTS["manager"],
        "storage_path": _storage_directory(_text(params.get("storage_path"))),
        # Only this workspace sees the runner; Galaxy's side stays the same.
        "job_runner": _text(params.get("job_runner")).lower() or DEFAULTS["job_runner"],
    }
    # Galaxy's jobs_directory is Pulsar's staging directory, always on the storage.
    settings["jobs_directory"] = f"{settings['storage_path']}/staging"
    for key in ("docker_enabled", "singularity_enabled", "cvmfs"):
        settings[key] = _boolean(params.get(key), DEFAULTS[key])
    name, manager = settings["name"], settings["manager"]
    # byoc-sync's naming.py: the RabbitMQ user and vhost derived from the BYOC name.
    mq_user = f"galaxy_{name}_byoc"
    settings.update(
        mq_user=mq_user,
        vhost=f"/pulsar/{mq_user}",
        # Pulsar's queue for new jobs: "<key prefix>_setup", prefix "pulsar_" for the
        # default manager and "pulsar_<manager>_" otherwise.
        setup_queue="pulsar__setup" if manager == "_default_" else f"pulsar_{manager}__setup",
        # No `?ssl=1` (byoc-sync's runner has it): kombu would let it replace the
        # amqp_connect_ssl_* settings that verify the certificate and hostname; those
        # settings switch TLS on by themselves.
        url=(
            f"pyamqp://{mq_user}:{settings['password']}@{settings['host']}:{settings['port']}"
            f"//pulsar/{mq_user}"
        ),
    )
    return settings


def pulsar_byoc_form(settings, cpus, memory_mb):
    """The ``byoc_pulsar`` form of Galaxy's preferences for this Pulsar, without the password.

    As the pulsar-byoc playbook's connection summary: every vCPU, 90 % of the memory in GB
    (at least 1 GB, the least byoc-sync accepts), no GPUs, and the storage directory as
    ``nfs_path``, from which byoc-sync derives ``jobs_directory`` (``/staging``),
    ``persistence_directory`` and the container cache.
    """
    return {
        "username": settings["name"],
        "max_accepted_cores": int(_text(cpus)),
        "max_accepted_mem": max(1, int(int(_text(memory_mb)) * 0.9 / 1024)),
        "min_accepted_gpus": 0,
        "max_accepted_gpus": 0,
        "nfs_path": settings["storage_path"],
    }


def _name_errors(name):
    """byoc-sync's rules for the BYOC name (validate.py's _name)."""
    if not _NAME.fullmatch(name):
        return "must be 3 to 32 of a-z 0-9 _, starting with a letter"
    if name.endswith("_") or "__" in name or name.endswith("_byoc"):
        return "must not end with _ or _byoc and must not contain __"
    return None


def pulsar_amqp_errors(settings):
    """Return one message per invalid parameter; the password is never included."""
    errors = []

    def shown(value):
        return _shown(str(value), settings["password"])

    host = settings["host"]
    if not _FQDN.fullmatch(host):
        errors.append(
            "pulsar_amqp_host must be the RabbitMQ hostname such as mq.galaxyproject.eu "
            f"(no https:// or port), got '{shown(host)}'"
        )

    port = settings["port"]
    if not (isinstance(port, int) and 1 <= port <= 65535):
        errors.append(
            "pulsar_amqp_port must be the broker's AMQPS port, an integer from 1 to 65535, "
            f"got '{shown(port)}'"
        )

    # Empty: generated by tasks/credentials.yml, which validates the result again.
    name = settings["name"]
    if name and (problem := _name_errors(name)):
        errors.append(f"pulsar_amqp_name {problem} (byoc-sync's rule), got '{shown(name)}'")

    if settings["password"] and not _PASSWORD.fullmatch(settings["password"]):
        errors.append(
            "pulsar_amqp_password must be 10 to 128 letters and digits (A-Z a-z 0-9), "
            "byoc-sync's rule, or empty for a generated one"
        )

    republish = settings["ack_republish_time"]
    if not (isinstance(republish, int) and republish >= 1):
        errors.append(
            "pulsar_amqp_ack_republish_time must be a whole number of seconds, at least 1 "
            f"(300 for byoc-sync's runner), got '{shown(republish)}'"
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
        not _STORAGE_PATH.fullmatch(storage_path)
        or os.path.normpath(storage_path) != storage_path
        or len(storage_path) > _NFS_PATH_MAX
    ):
        errors.append(
            "pulsar_storage_path must be an attached storage volume's mount point, "
            "/data/<volume name>, or a directory on it, /data/<volume name>/<directory>, "
            f"of A-Z a-z 0-9 _ . - without . or .., at most {_NFS_PATH_MAX} characters, "
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
            "pulsar_byoc_form": pulsar_byoc_form,
        }
