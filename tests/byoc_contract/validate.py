"""Strict validation of user-entered BYOC form values.

Everything that reaches a playbook file passes through here first, so the rules are deliberately narrow:
user text ends up inside YAML, Jinja templates and an AMQP URL.
"""

from __future__ import annotations

import math
import re
from typing import Any

from .models import ByocRequest, UserRecord, ValidationFailure

FIELDS = (
    "username",
    "password",
    "max_accepted_cores",
    "max_accepted_mem",
    "min_accepted_gpus",
    "max_accepted_gpus",
    "nfs_path",
)
REQUIRED = ("username", "password", "max_accepted_cores", "max_accepted_mem")

NAME_RE = re.compile(r"^[a-z][a-z0-9_]{2,31}$")
PASSWORD_RE = re.compile(r"^[A-Za-z0-9]{10,128}$")
PATH_RE = re.compile(r"^/[A-Za-z0-9._/-]*$")
# Plain ASCII decimals only: float() would also accept "1e1", "1_0", "+8" or full-width digits.
NUMBER_RE = re.compile(r"[0-9]+(\.[0-9]+)?")

LIMITS = {
    "max_accepted_cores": (1, 1024),
    "min_accepted_gpus": (0, 64),
    "max_accepted_gpus": (0, 64),
}
MIN_MEM_GB = 0.1
MAX_MEM_GB = 16384


def _clean(value: Any) -> Any:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and not NUMBER_RE.fullmatch(value):
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _int_field(name: str, value: Any, errors: dict[str, str]) -> int | None:
    low, high = LIMITS[name]
    number = _number(value)
    if number is None or not number.is_integer():
        errors[name] = "must be a whole number"
        return None
    if not low <= number <= high:
        errors[name] = f"must be between {low} and {high}"
        return None
    return int(number)


def _mem_field(value: Any, errors: dict[str, str]) -> int | float | None:
    number = _number(value)
    if number is None:
        errors["max_accepted_mem"] = "must be a number (GB)"
        return None
    if not MIN_MEM_GB <= number <= MAX_MEM_GB:
        errors["max_accepted_mem"] = f"must be between {MIN_MEM_GB} and {MAX_MEM_GB} (GB)"
        return None
    return int(number) if number.is_integer() else number


def _name(value: Any, errors: dict[str, str]) -> str | None:
    if not isinstance(value, str) or not NAME_RE.fullmatch(value):
        errors["username"] = "must be 3-32 characters: lowercase letters, digits and '_', starting with a letter"
        return None
    if value.endswith("_") or "__" in value or value.endswith("_byoc"):
        errors["username"] = "must not end with '_' or '_byoc' and must not contain '__'"
        return None
    return value


def is_valid_name(value: str) -> bool:
    """True for a BYOC name byoc-sync could have generated objects for."""
    errors: dict[str, str] = {}
    return _name(value, errors) is not None


def _password(value: Any, errors: dict[str, str]) -> str | None:
    if not isinstance(value, str) or not PASSWORD_RE.fullmatch(value):
        errors["password"] = "must be 10-128 characters, letters and digits only"
        return None
    return value


def _nfs_path(value: Any, errors: dict[str, str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 255 or not PATH_RE.fullmatch(value):
        errors["nfs_path"] = "must be an absolute path of letters, digits, '.', '_', '-' and '/' (max 255)"
        return None
    stripped = value.rstrip("/")
    if not stripped or "//" in value or any(part in (".", "..") for part in stripped.split("/")[1:]):
        errors["nfs_path"] = "must not be '/', contain '//' or '.'/'..' segments"
        return None
    return stripped


def validate(record: UserRecord, section: str = "byoc_pulsar") -> ByocRequest | ValidationFailure | None:
    """Return the validated request, a failure, or ``None`` when the user has not filled in the section."""
    prefs = record.prefs or {}
    values = {f: _clean(prefs.get(f"{section}|{f}")) for f in FIELDS}
    if all(v is None for v in values.values()):
        return None

    errors: dict[str, str] = {}
    for f in REQUIRED:
        if values[f] is None:
            errors[f] = "is required"

    name = _name(values["username"], errors) if values["username"] is not None else None
    password = _password(values["password"], errors) if values["password"] is not None else None
    cores = (
        _int_field("max_accepted_cores", values["max_accepted_cores"], errors)
        if values["max_accepted_cores"] is not None
        else None
    )
    mem = _mem_field(values["max_accepted_mem"], errors) if values["max_accepted_mem"] is not None else None
    min_gpus = _int_field("min_accepted_gpus", values["min_accepted_gpus"] or 0, errors)
    max_gpus = _int_field("max_accepted_gpus", values["max_accepted_gpus"] or 0, errors)
    if min_gpus is not None and max_gpus is not None and min_gpus > max_gpus:
        errors["min_accepted_gpus"] = "must not be greater than max_accepted_gpus"
    nfs_path = _nfs_path(values["nfs_path"], errors)

    if errors:
        return ValidationFailure(errors)
    assert name and password and cores is not None and mem is not None
    return ByocRequest(
        name=name,
        password=password,
        max_cores=cores,
        max_mem=mem,
        min_gpus=min_gpus or 0,
        max_gpus=max_gpus or 0,
        nfs_path=nfs_path,
    )
