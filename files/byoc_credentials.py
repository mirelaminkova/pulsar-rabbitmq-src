"""Load or create this Pulsar's BYOC name and password, kept on the storage volume.

Run as root by tasks/credentials.yml, with the system Python in isolated mode:

    python3 -I byoc_credentials.py STORE [--no-write] < '{"name": "...", "password": "..."}'

STORE is the credentials file (<storage directory>/byoc.json, root-only). The JSON on stdin
holds the component parameters pulsar_amqp_name and pulsar_amqp_password; a given value
wins over the stored one, an empty one means the stored value, or a new one when there is
none: a name of "src" and 8 lowercase hex digits, a password of 24 letters and digits, both
from the `secrets` module. What is in effect must follow byoc-sync's rules and is written to
STORE (created 0600, replaced atomically), unless --no-write (Ansible's check mode).

Prints {"name": ..., "password": ..., "changed": true|false} on stdout. Exit status 1 with
one line on stderr, never containing a name or password, when STORE is not a file this
script wrote (another owner, a link, readable by others, not its JSON), cannot be read or
written, or when a given or stored value does not follow byoc-sync's rules.
"""

import argparse
import json
import os
import re
import secrets
import stat
import string
import sys
import tempfile

# byoc-sync's validate.py: NAME_RE and the name's extra rules, PASSWORD_RE.
NAME_RE = re.compile(r"[a-z][a-z0-9_]{2,31}")
PASSWORD_RE = re.compile(r"[A-Za-z0-9]{10,128}")
PASSWORD_ALPHABET = string.ascii_letters + string.digits
PASSWORD_LENGTH = 24
KEYS = ("name", "password")


class CredentialsError(Exception):
    """A problem with the store or its content; the message never holds a value."""


def generate_name():
    return "src" + secrets.token_hex(4)


def generate_password():
    return "".join(secrets.choice(PASSWORD_ALPHABET) for _ in range(PASSWORD_LENGTH))


def is_valid_name(name):
    return bool(
        NAME_RE.fullmatch(name)
        and not name.endswith("_")
        and "__" not in name
        and not name.endswith("_byoc")
    )


def is_valid_password(password):
    return bool(PASSWORD_RE.fullmatch(password))


def _credentials(value, what):
    """``value`` as {"name": str, "password": str}, or a CredentialsError naming ``what``."""
    if not (isinstance(value, dict) and set(value) == set(KEYS)) or not all(
        isinstance(value[key], str) for key in KEYS
    ):
        raise CredentialsError(f"{what} is not the credentials file this component writes")
    return {key: value[key].strip() for key in KEYS}


def read_store(path):
    """The stored credentials, or None when there is no store yet."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise CredentialsError(f"cannot read {path}: {error.strerror}") from None
    if not stat.S_ISREG(info.st_mode):
        raise CredentialsError(f"{path} must be a regular file, not a link or a directory")
    if info.st_uid != os.geteuid():
        raise CredentialsError(f"{path} must be owned by the user running the deployment")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise CredentialsError(f"{path} must have mode 0600: it holds a password")
    try:
        with open(path) as handle:
            stored = _credentials(json.load(handle), path)
    except OSError as error:
        raise CredentialsError(f"cannot read {path}: {error.strerror}") from None
    except ValueError:
        raise CredentialsError(
            f"{path} is not the credentials file this component writes"
        ) from None
    if not is_valid_name(stored["name"]) or not is_valid_password(stored["password"]):
        raise CredentialsError(
            f"the name or password stored in {path} does not follow byoc-sync's rules; "
            "remove the file for new ones, or set pulsar_amqp_name and pulsar_amqp_password"
        )
    return stored


def write_store(path, credentials):
    """Write the credentials to ``path`` atomically, readable by the owner only."""
    directory = os.path.dirname(path)
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".byoc-", dir=directory)
    except OSError as error:
        raise CredentialsError(f"cannot write {path}: {error.strerror}") from None
    try:
        # mkstemp creates the file 0600. Written to disk before it replaces the store: a
        # truncated store after a crash would cost the Galaxy account its credentials.
        with os.fdopen(descriptor, "w") as handle:
            json.dump(credentials, handle)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        # The rename too, so that the store the playbook goes on with survives a crash.
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        os.unlink(temporary)
        raise CredentialsError(f"cannot write {path}: {error.strerror}") from None


def resolve(path, given, write=True):
    """The credentials in effect: given, else stored, else generated; stored when changed."""
    # The playbook validates the parameters first; this keeps a bad value out of the store
    # for any other caller, so that no later run refuses the store.
    if given["name"] and not is_valid_name(given["name"]):
        raise CredentialsError(
            "the given name (pulsar_amqp_name) does not follow byoc-sync's rules"
        )
    if given["password"] and not is_valid_password(given["password"]):
        raise CredentialsError(
            "the given password (pulsar_amqp_password) does not follow byoc-sync's rules"
        )
    stored = read_store(path)
    effective = {
        "name": given["name"] or (stored or {}).get("name") or generate_name(),
        "password": given["password"] or (stored or {}).get("password") or generate_password(),
    }
    changed = effective != stored
    if changed and write:
        write_store(path, effective)
    return {**effective, "changed": changed}


def main(argv=None, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("store", help="the credentials file on the storage volume")
    parser.add_argument("--no-write", action="store_true", help="only report (check mode)")
    args = parser.parse_args(argv)
    try:
        try:
            given = _credentials(json.load(stdin), "stdin")
        except ValueError:
            raise CredentialsError(
                "stdin is not the credentials file this component writes"
            ) from None
        result = resolve(args.store, given, write=not args.no_write)
    except CredentialsError as error:
        print(error, file=stderr)
        return 1
    json.dump(result, stdout)
    stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
