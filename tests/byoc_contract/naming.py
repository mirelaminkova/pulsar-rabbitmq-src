"""Names of every object byoc-sync owns, derived from the user's BYOC name.

``{x}`` is the validated name (``[a-z0-9_]``); ``{x_dash}`` is the same with ``_`` replaced by ``-``
(usegalaxy.eu convention: dashes for TPV tags, underscores for destinations).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields

from .validate import is_valid_name

_X = r"(?P<x>[a-z][a-z0-9_]*)"
_XD = r"(?P<xd>[a-z][a-z0-9-]*)"
SINGLE_BRANCH = "byoc_ci_batch"  # the --single-pr branch


@dataclass(frozen=True)
class NamingConfig:
    mq_user: str = "galaxy_{x}_byoc"
    vhost: str = "/pulsar/galaxy_{x}_byoc"
    vault_var: str = "rabbitmq_password_galaxy_{x}_byoc"
    runner_id: str = "pulsar_eu_{x}_byoc"
    destination_id: str = "pulsar_{x}_byoc_tpv"
    tag: str = "{x_dash}-byoc-pulsar"
    rule_id: str = "byoc-pulsar-{x_dash}"
    branch: str = "{x}_byoc_ci"

    def name_from(self, kind: str, value: str) -> str | None:
        """Reverse a pattern: the BYOC name an object id belongs to, or ``None`` if it is not ours."""
        if kind not in {f.name for f in fields(self)}:
            raise ValueError(f"unknown naming kind {kind!r}")
        pattern = getattr(self, kind)
        regex = re.escape(pattern).replace(r"\{x\}", _X).replace(r"\{x_dash\}", _XD)
        match = re.fullmatch(regex, value)
        if not match:
            return None
        name = match.groupdict().get("x") or (match.groupdict().get("xd") or "").replace("-", "_")
        return name if is_valid_name(name) else None


@dataclass(frozen=True)
class Names:
    name: str
    mq_user: str
    vhost: str
    vault_var: str
    runner_id: str
    destination_id: str
    tag: str
    rule_id: str
    branch: str

    @classmethod
    def for_(cls, name: str, cfg: NamingConfig) -> Names:
        subst = {"x": name, "x_dash": name.replace("_", "-")}
        return cls(name=name, **{f.name: getattr(cfg, f.name).format(**subst) for f in fields(NamingConfig)})
