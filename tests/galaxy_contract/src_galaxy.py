"""Filters for parsing SURF ResearchCloud (SRC) component parameters.

Loaded automatically by Ansible because this directory sits next to galaxysrv.yml.

SRC passes list parameters such as ``src_galaxy_pulsar_configs`` as one string in a
relaxed YAML flow style, e.g. ``[{name: hpc, url: https://h.example.org, docker_enabled: true}]``.
:func:`src_config_list` turns such a string into a list of dicts; every list parameter
of this playbook goes through it.
"""

import json
import re

from ansible.errors import AnsibleFilterError

# Quote bare keys: `{name:` / `, name:` -> `{"name":`.
_BARE_KEY = re.compile(r"([{,]\s*)(\w+):")
# Quote bare scalar values up to the next `,`, `}` or `]`.
_BARE_VALUE = re.compile(r":\s*([^,\[\]{}\"'\s][^,\[\]{}]*?)(\s*[,}\]])")
# Bare true/false were quoted by _BARE_VALUE; turn them back into booleans.
_QUOTED_BOOL = re.compile(r':\s*"(true|false)"')


def src_config_list(value, parameter="value"):
    """Parse an SRC list-of-dicts parameter; ``None``, ``''`` and ``'[]'`` give ``[]``."""
    if isinstance(value, list):
        data = value
    elif value is None or (isinstance(value, str) and value.strip() in ("", "[]")):
        return []
    elif isinstance(value, str):
        text = _BARE_KEY.sub(r'\1"\2":', value)
        text = _BARE_VALUE.sub(r': "\1"\2', text)
        text = _QUOTED_BOOL.sub(r": \1", text)
        try:
            data = json.loads(text)
        except ValueError as error:
            raise AnsibleFilterError(
                f"{parameter}: expected a list of {{key: value}} mappings such as "
                f"'[{{name: x}}]', but it could not be parsed ({error.msg})."
            ) from None
    else:
        data = value
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise AnsibleFilterError(
            f"{parameter}: expected a list of {{key: value}} mappings such as '[{{name: x}}]'."
        )
    return data


# --- src_galaxy_pulsar_amqp_configs (issue #4) -----------------------------------

AMQP_DEFAULTS = {
    "docker_enabled": False,
    "singularity_enabled": False,
    "dependency_resolution": "remote",
    "manager": "_default_",
    "jobs_directory": "/mnt/pulsar/files/staging",
}
AMQP_KEYS = {"name", "label", "password", *AMQP_DEFAULTS}
_NAME = re.compile(r"[a-z0-9_-]+")
_MANAGER = re.compile(r"[A-Za-z0-9_-]+")
_JOBS_DIRECTORY = re.compile(r"/\S*")
# Passwords end up verbatim in amqp_url / message_queue_url, so only URL-unreserved characters.
_PASSWORD = re.compile(r"[A-Za-z0-9._~-]{12,}")
_RESERVED_NAMES = ("galaxy", "guest")  # existing RabbitMQ users
_BOOLEANS = {True: True, False: False, "true": True, "false": False}
_DEPENDENCY_RESOLUTION = ("none", "local", "remote")


def _entry_errors(entry, names):
    if not isinstance(entry, dict):
        return ["must be a mapping"]
    if "name" not in entry:
        return ["'name' is required"]
    name = entry["name"]
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        return ["'name' must match [a-z0-9_-]+ (lowercase letters, digits, '_' and '-')"]
    if len(name) > 64:
        return ["'name' must be at most 64 characters"]
    if name in _RESERVED_NAMES:
        return [f"'name' must not be '{name}' (an existing RabbitMQ user)"]
    if name in names:
        return [f"duplicate name '{name}'"]
    names.add(name)

    errors = [f"unknown key '{key}'" for key in sorted(set(entry) - AMQP_KEYS)]
    for key in ("docker_enabled", "singularity_enabled"):
        if key in entry and not _is_boolean(entry[key]):
            errors.append(f"'{key}' must be true or false")
    if entry.get("dependency_resolution", "remote") not in _DEPENDENCY_RESOLUTION:
        errors.append(f"'dependency_resolution' must be one of {', '.join(_DEPENDENCY_RESOLUTION)}")
    if not _matches(_MANAGER, entry.get("manager", "_default_")):
        errors.append("'manager' must match [A-Za-z0-9_-]+")
    if not _matches(_JOBS_DIRECTORY, entry.get("jobs_directory", "/")):
        errors.append("'jobs_directory' must be an absolute path without spaces")
    if "label" in entry and not (isinstance(entry["label"], str) and entry["label"].strip()):
        errors.append("'label' must be a non-empty string")
    if "password" in entry and not _matches(_PASSWORD, entry["password"]):
        # Never echo the value: it is a secret.
        errors.append(
            "'password' must be at least 12 characters from A-Z a-z 0-9 . _ ~ - "
            "(it is put into URLs unescaped); leave it out to generate one"
        )
    return [f"({name}): {error}" for error in errors]


def _is_boolean(value):
    # Not `value in _BOOLEANS`: 1 == True and 0 == False.
    return isinstance(value, bool) or value in ("true", "false")


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def pulsar_amqp_config_errors(entries):
    """Return one message per problem in parsed ``src_galaxy_pulsar_amqp_configs``."""
    names = set()
    messages = []
    for number, entry in enumerate(entries, start=1):
        for error in _entry_errors(entry, names):
            separator = " " if error.startswith("(") else ": "
            messages.append(f"entry {number}{separator}{error}")
    return messages


def pulsar_amqp_configs(entries):
    """Apply defaults and derive RabbitMQ/job config names; expects entries without errors."""
    configs = []
    for entry in entries:
        name = entry["name"]
        config = {**AMQP_DEFAULTS, **entry, "label": entry.get("label", name)}
        for key in ("docker_enabled", "singularity_enabled"):
            config[key] = _BOOLEANS[config[key]]
        config.update(
            vhost=f"/pulsar/{name}",
            runner=f"pulsar_mq_{name}",
            environment=f"amqp_{name}",
        )
        configs.append(config)
    return configs


def galaxy_web_client_requirement(version):
    """Return the pip requirement for the prebuilt client of Galaxy ``version`` (26.1+).

    Same rule as Galaxy's ``scripts/common_startup.sh``: a tagged release needs exactly its own
    ``galaxy-web-client`` wheel; an untagged ``.devN``/``rcN`` version (a ``release_*`` branch
    tip) has none on PyPI, so take the latest release below it.
    """
    version = version.strip()
    release = re.sub(r"\.?(dev|rc)\d*.*$", "", version)
    if release != version:
        return f"galaxy-web-client<{release}"
    return f"galaxy-web-client=={version}"


class FilterModule:
    def filters(self):
        return {
            "src_config_list": src_config_list,
            "pulsar_amqp_configs": pulsar_amqp_configs,
            "pulsar_amqp_config_errors": pulsar_amqp_config_errors,
            "galaxy_web_client_requirement": galaxy_web_client_requirement,
        }
