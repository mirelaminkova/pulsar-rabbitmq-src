#!/usr/bin/env python3
"""Check that pulsar_storage_path is a usable directory on an attached storage volume.

    check_storage.py [--protect] [--docker] /data/<volume name>/pulsar

Run as root: by the playbook before anything is installed (with --protect), and by the
pulsar, slurmd and (with --docker) Docker units before every start, so that none writes
to the empty mount point on the root disk when the volume is missing, however SRC
mounts it.
On an accepted volume, --protect sets the sticky bit on a volume root that others can
write to, so that they cannot rename the storage directory; it prints what it changed.

Exit status 0 when the path can hold Pulsar's data, 1 with the reason on stdout when it
cannot, 2 for wrong usage. The path need not exist yet; its deepest existing ancestor is
checked. Refused are a path

* with a symbolic link in it (root creates the directories below it and hands some to
  the pulsar user);
* on the root file system, which includes SRC's mount point directory before the volume
  is attached, or a bind mount of a root file system directory;
* that is the volume's mount point itself (the playbook makes the storage directory
  root's);
* on a file system that does not keep its data (tmpfs and the like), or, for Docker's
  data-root, a network or FUSE file system (overlay2 does not work there);
* below a directory another user could rename it from: below the mount point, every
  directory must be root's, and none, the mount point included, may be writable by
  group or others without the sticky bit;
* where root cannot create a file and give it to another user (read-only, NFS
  root_squash, FUSE without allow_other), as the playbook must for the pulsar user.

Ansible's mount facts give neither the device of a path nor its owners along the way, so
this is a script rather than a few tasks.
"""

import os
import re
import stat
import sys
import tempfile

MOUNTINFO = "/proc/self/mountinfo"
ROOT_UID = 0
NOBODY = 65534
# File systems that lose their content, or cannot be written, so never a storage volume.
NOT_PERSISTENT = ("tmpfs", "ramfs", "devtmpfs", "overlay", "squashfs")
# Docker's overlay2 storage driver needs a local file system under its data-root.
NOT_FOR_DOCKER = ("nfs", "cifs", "smb3", "fuse", "ceph", "glusterfs", "9p", "virtiofs")
ATTACH = (
    "attach a storage volume to the workspace and set pulsar_storage_path to a directory "
    "on it, such as /data/<volume name>/pulsar"
)


def _decode(field):
    """Undo mountinfo's octal escapes: \\040 for a space, also tab, newline and backslash."""
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), field)


def mounts(path):
    """Return ``[(device, mount point, fstype)]`` from a mountinfo file, in mount order.

    ``device`` is the ``major:minor`` field; non-UTF-8 bytes survive as surrogates.
    """
    result = []
    with open(path, encoding="utf-8", errors="surrogateescape") as lines:
        for line in lines:
            fields, _, rest = line.partition(" - ")
            fields = fields.split()
            result.append((fields[2], _decode(fields[4]), rest.split()[0]))
    return result


def mount_of(path, device, mount_list):
    """The mount that holds ``path``: the innermost mount on its ancestors, of several on
    one point the last; those with the path's device first (another device there was
    over-mounted), any device when none has it (a btrfs subvolume's ``st_dev`` is not
    mountinfo's). ``("/", "")`` when there is none."""
    for same_device in (True, False):
        best = None
        for dev, point, fstype in mount_list:
            inside = point in ("/", path) or path.startswith(point + "/")
            matches = inside and (dev == device or not same_device)
            if matches and (best is None or len(point) >= len(best[0])):
                best = (point, fstype)
        if best is not None:
            return best
    return ("/", "")


def root_device():
    return os.stat("/").st_dev


def _existing_directory(path):
    """The deepest existing directory of ``path``, or why there is none to check."""
    existing = path
    while True:
        try:
            os.lstat(existing)
            break
        except FileNotFoundError:
            existing = os.path.dirname(existing)
        except OSError as error:
            return None, f"pulsar_storage_path {path}: cannot examine {existing} ({error.strerror})"
    # realpath also resolves links in the ancestors; an unchanged path has none.
    if os.path.realpath(existing) != existing:
        return None, (
            f"pulsar_storage_path {path} contains a symbolic link ({existing} is or "
            "is below one); use the volume's real path"
        )
    if not os.path.isdir(existing):
        return None, f"pulsar_storage_path {path}: {existing} is not a directory"
    return existing, None


def _volume_problem(path, existing, mount, device, docker):
    """Why the file system holding ``existing`` is not an attached volume, if it is not."""
    point, fstype = mount
    if point == "/" or os.stat(existing).st_dev == device:
        return f"pulsar_storage_path {path} is on the root file system: {ATTACH}"
    if path == point:
        return (
            f"pulsar_storage_path {path} is the volume's mount point; "
            f"set it to a directory on the volume, such as {path}/pulsar"
        )
    if fstype in NOT_PERSISTENT:
        return f"pulsar_storage_path {path} is on a {fstype} file system, not a volume: {ATTACH}"
    if docker and fstype.startswith(NOT_FOR_DOCKER):
        return (
            f"pulsar_storage_path {path} is on a {fstype} file system, where Docker's "
            "data-root does not work; use a block storage volume or set pulsar_docker to false"
        )
    return None


def _ownership_problem(path, existing, point):
    """Whether a user other than root could swap a directory of ``existing`` for another."""
    directory = existing
    while True:
        info = os.lstat(directory)
        if directory != point and info.st_uid != ROOT_UID:
            return (
                f"pulsar_storage_path {path}: {directory} belongs to uid {info.st_uid}, "
                "not root; choose a new directory or give it to root (chown root:root)"
            )
        if not _safe_from_others(info.st_mode):
            return (
                f"pulsar_storage_path {path}: other users can write to {directory} and so "
                f"replace Pulsar's directory; remove their write access or set the sticky "
                f"bit (chmod +t {directory})"
            )
        if directory == point:
            return None
        directory = os.path.dirname(directory)


def _can_give_away(probe):
    """Whether root can make ``probe`` another user's, as it must for the pulsar user."""
    try:
        os.lchown(probe, NOBODY, -1)
    except OSError:
        return False
    return os.lstat(probe).st_uid == NOBODY


def _write_problem(path, existing):
    try:
        handle, probe = tempfile.mkstemp(dir=existing, prefix=".pulsar-storage-check-")
    except OSError as error:
        return f"pulsar_storage_path {path} is not writable ({existing}: {error.strerror})"
    os.close(handle)
    try:
        if not _can_give_away(probe):
            return (
                f"pulsar_storage_path {path}: root cannot give files on this volume to "
                "other users (NFS root_squash or FUSE without allow_other?)"
            )
    finally:
        os.unlink(probe)
    return None


def _safe_from_others(mode):
    return not mode & (stat.S_IWGRP | stat.S_IWOTH) or mode & stat.S_ISVTX


def protect(point):
    """Set the sticky bit on ``point`` if others can write to it; return what changed."""
    mode = os.lstat(point).st_mode
    if _safe_from_others(mode):
        return None
    os.chmod(point, stat.S_IMODE(mode) | stat.S_ISVTX)
    return f"set the sticky bit on {point}, so that other users cannot rename Pulsar's directory"


def check(path, mountinfo=MOUNTINFO, device=None, docker=False, protect_mount=False):
    """Return why ``path`` cannot hold Pulsar's data, or None when it can.

    With ``protect_mount``, the volume root gets the sticky bit first (see protect)."""
    if not os.path.isabs(path) or os.path.normpath(path) != path:
        return f"pulsar_storage_path must be an absolute path without . or .., got '{path}'"
    existing, problem = _existing_directory(path)
    if problem:
        return problem
    # After the stat in _existing_directory, which also mounts an automounted volume.
    info = os.stat(existing)
    mount = mount_of(
        existing, f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}", mounts(mountinfo)
    )
    device = root_device() if device is None else device
    problem = _volume_problem(path, existing, mount, device, docker)
    if problem:
        return problem
    # Only on a volume the path may use (not the root file system, not tmpfs).
    if protect_mount:
        changed = protect(mount[0])
        if changed:
            print(changed)
    return _ownership_problem(path, existing, mount[0]) or _write_problem(path, existing)


def main(argv):
    options = set()
    while argv[:1] and argv[0] in ("--docker", "--protect"):
        options.add(argv.pop(0))
    if len(argv) != 1:
        print("usage: check_storage.py [--protect] [--docker] PATH")
        return 2
    problem = check(
        argv[0], MOUNTINFO, root_device(), "--docker" in options, "--protect" in options
    )
    if problem:
        print(problem)
        return 1
    print(f"{argv[0]} is on an attached storage volume")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
