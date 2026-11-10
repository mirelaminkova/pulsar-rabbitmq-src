"""The volume check files/check_storage.py, run unprivileged.

The test user plays root (ROOT_UID) and the user the probe file is given to (NOBODY),
which needs no privilege; mountinfo files describe temporary directories as volumes.
"""

import errno
import importlib.util
import os
import runpy
import stat
import subprocess
import sys

import pytest

from tests.helpers import REPO

SCRIPT = REPO / "files" / "check_storage.py"

_SPEC = importlib.util.spec_from_file_location("check_storage", SCRIPT)
check_storage = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(check_storage)


ESCAPED_SPACE = "\\040"


def device_of(path):
    info = os.stat(path)
    return f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"


def mountinfo(tmp_path, *mounts):
    """A /proc/self/mountinfo with ``(mount point, fstype[, device])`` lines; the device
    defaults to that of the mount point, as for a real mount."""
    lines = []
    for n, (point, fstype, *device) in enumerate(mounts, start=20):
        dev = device[0] if device else device_of(point)
        escaped = point.replace(" ", ESCAPED_SPACE)
        lines.append(f"{n} 1 {dev} / {escaped} rw shared:1 - {fstype} dev rw")
    path = tmp_path / "mountinfo"
    path.write_text("\n".join(lines) + "\n")
    return str(path)


@pytest.fixture(autouse=True)
def as_root(monkeypatch):
    """The tests run unprivileged: the test user plays root, and the give-away probe
    gives files to that user, which needs no privilege."""
    monkeypatch.setattr(check_storage, "ROOT_UID", os.getuid())
    monkeypatch.setattr(check_storage, "NOBODY", os.getuid())


ROOT_FS = ("/", "ext4", "0:1")


@pytest.fixture
def volume(tmp_path):
    """A directory that mountinfo reports as an ext4 volume, on another device than /."""
    root = tmp_path / "vol"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    return root, mountinfo(tmp_path, ROOT_FS, (str(root), "ext4"))


def check(path, info, device=-1, docker=False):
    return check_storage.check(str(path), mountinfo=info, device=device, docker=docker)


def test_a_directory_on_a_volume_passes(volume):
    root, info = volume
    assert check(root / "pulsar", info) is None
    (root / "pulsar").mkdir(mode=0o755)
    assert check(root / "pulsar", info) is None
    assert os.listdir(root / "pulsar") == []  # the probe leaves nothing behind


def test_missing_parents_are_fine(volume):
    root, info = volume
    assert check(root / "a" / "b" / "pulsar", info) is None


def test_root_filesystem_is_refused(tmp_path):
    (tmp_path / "pulsar").mkdir()
    message = check(tmp_path / "pulsar", mountinfo(tmp_path, ("/", "ext4")))
    assert "on the root file system" in message
    assert "attach a storage volume" in message


def test_unattached_mount_point_is_refused(tmp_path):
    # SRC's mount point directory exists, but the volume is not mounted on it.
    root = tmp_path / "vol"
    root.mkdir()
    info = mountinfo(tmp_path, ROOT_FS, (str(tmp_path / "vol2"), "ext4", "8:16"))
    assert "on the root file system" in check(root / "pulsar", info)


def test_a_mount_of_another_device_does_not_count(tmp_path):
    # SRC's mount point directory on the root disk, with a volume over-mounted on its
    # parent: the path's own device (the root disk's) decides.
    root = tmp_path / "vol"
    root.mkdir(mode=0o755)
    info = mountinfo(tmp_path, ("/", "ext4"), (str(tmp_path), "tmpfs", "0:99"))
    assert "on the root file system" in check(root / "pulsar", info)


def test_without_a_mount_of_its_device_the_innermost_mount_counts(tmp_path):
    # A btrfs subvolume's st_dev is none of mountinfo's devices.
    root = tmp_path / "vol"
    root.mkdir(mode=0o755)
    info = mountinfo(tmp_path, ROOT_FS, (str(root), "btrfs", "0:77"))
    assert check(root / "pulsar", info) is None
    root_info = mountinfo(tmp_path, ("/", "ext4", "0:1"))
    assert "on the root file system" in check(root / "pulsar", root_info)


def test_same_device_as_root_is_refused(volume):
    # A bind mount of a directory of the root file system is not a volume.
    root, info = volume
    message = check(root / "pulsar", info, device=os.stat(root).st_dev)
    assert "on the root file system" in message


def test_check_compares_with_the_device_of_slash_by_default(volume, monkeypatch):
    root, info = volume
    monkeypatch.setattr(check_storage, "root_device", lambda: os.stat(root).st_dev)
    assert "root file system" in check_storage.check(str(root / "pulsar"), mountinfo=info)


def test_the_mount_point_itself_is_refused(volume):
    root, info = volume
    message = check(root, info)
    assert "is the volume's mount point" in message
    assert f"{root}/pulsar" in message


@pytest.mark.parametrize("fstype", ["tmpfs", "ramfs", "overlay", "squashfs", "devtmpfs"])
def test_non_persistent_file_systems_are_refused(tmp_path, fstype):
    root = tmp_path / "vol"
    root.mkdir()
    info = mountinfo(tmp_path, ROOT_FS, (str(root), fstype))
    assert f"a {fstype} file system" in check(root / "pulsar", info)


@pytest.mark.parametrize(
    "fstype", ["nfs", "nfs4", "cifs", "fuse.rclone", "fuseblk", "ceph", "9p", "virtiofs"]
)
def test_network_file_systems_are_refused_for_docker_only(volume, tmp_path, fstype):
    root, block = volume
    network = mountinfo(tmp_path, ROOT_FS, (str(root), fstype))
    assert check(root / "pulsar", network) is None
    assert "Docker's data-root" in check(root / "pulsar", network, docker=True)
    block = mountinfo(tmp_path, ROOT_FS, (str(root), "xfs"))
    assert check(root / "pulsar", block, docker=True) is None


def test_the_innermost_and_latest_mount_wins(tmp_path):
    root = tmp_path / "vol"
    (root / "sub").mkdir(mode=0o755, parents=True)
    root.chmod(0o755)
    info = mountinfo(
        tmp_path,
        ROOT_FS,
        (str(root), "tmpfs"),
        (str(root), "ext4"),
        (str(root / "sub"), "tmpfs", "0:99"),
    )
    assert check(root / "pulsar", info) is None
    assert check(root / "sub" / "pulsar", info) is None


def test_escaped_mount_points_are_decoded(tmp_path):
    root = tmp_path / "my vol"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    info = mountinfo(tmp_path, ROOT_FS, (str(root), "ext4"))
    assert check(root / "pulsar", info) is None


def test_non_utf8_mountinfo_is_read(volume):
    root, info = volume
    with open(info, "ab") as lines:
        lines.write(b"99 1 0:42 / /media/\xff rw - ext4 dev rw\n")
    assert check(root / "pulsar", info) is None


@pytest.mark.parametrize("value", ["relative/pulsar", "/data/vol/../pulsar", "/data/vol/pulsar/"])
def test_paths_must_be_normal_and_absolute(volume, value):
    _, info = volume
    assert "absolute path without" in check(value, info)


def test_symbolic_links_are_refused(volume):
    root, info = volume
    (root / "real").mkdir()
    (root / "link").symlink_to(root / "real")
    message = check(root / "link" / "pulsar", info)
    assert "symbolic link" in message
    assert str(root / "link") in message


@pytest.mark.parametrize("target", ["/etc", "missing"])
def test_a_link_at_the_path_itself_is_refused(volume, target):
    root, info = volume
    (root / "pulsar").symlink_to(target)
    assert "symbolic link" in check(root / "pulsar", info)


def test_a_file_in_the_way_is_refused(volume):
    root, info = volume
    (root / "pulsar").write_text("")
    assert "is not a directory" in check(root / "pulsar", info)


def test_an_unreadable_parent_is_refused(volume, monkeypatch):
    # As on NFS with root_squash: root may not even look.
    root, info = volume
    real_lstat = os.lstat

    def lstat(path):
        if str(path) == str(root / "a" / "pulsar"):
            raise PermissionError(errno.EACCES, "Permission denied")
        return real_lstat(path)

    monkeypatch.setattr(check_storage.os, "lstat", lstat)
    assert "cannot examine" in check(root / "a" / "pulsar", info)


def test_a_directory_of_another_user_is_refused(volume, monkeypatch):
    root, info = volume
    (root / "pulsar").mkdir(mode=0o755)
    monkeypatch.setattr(check_storage, "ROOT_UID", os.getuid() + 1)
    message = check(root / "pulsar", info)
    assert f"{root}/pulsar belongs to uid {os.getuid()}" in message
    # The mount point itself may belong to someone else (SRC decides that).
    assert check(root / "new", info) is None


@pytest.mark.parametrize("mode", [0o775, 0o757, 0o777])
def test_directories_others_can_write_are_refused(volume, mode):
    root, info = volume
    (root / "a").mkdir()
    (root / "a").chmod(mode)
    assert f"other users can write to {root}/a" in check(root / "a" / "pulsar", info)
    root.chmod(mode)
    assert f"chmod +t {root}" in check(root / "pulsar", info)


def test_sticky_writable_directories_are_fine(volume):
    root, info = volume
    root.chmod(0o777 | stat.S_ISVTX)
    assert check(root / "pulsar", info) is None


def test_unwritable_storage_is_refused(volume, monkeypatch):
    root, info = volume

    def refuse(*args, **kwargs):
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(check_storage.tempfile, "mkstemp", refuse)
    message = check(root / "pulsar", info)
    assert "is not writable" in message
    assert "Read-only file system" in message


@pytest.mark.parametrize("change_takes", [False, True])
def test_storage_where_root_cannot_give_files_away_is_refused(volume, monkeypatch, change_takes):
    # Unprivileged, giving a file to another uid fails, as for root on NFS with
    # root_squash; or the change seems to work but does not take, as on some FUSE mounts.
    root, info = volume
    monkeypatch.setattr(check_storage, "NOBODY", os.getuid() + 1)
    if change_takes:
        monkeypatch.setattr(check_storage.os, "lchown", lambda *args: None)
    assert "cannot give files" in check(root / "pulsar", info)
    assert os.listdir(root) == []


def test_script_reports_and_sets_the_exit_status(tmp_path):
    def run(*args):
        return subprocess.run(  # noqa: S603 - fixed command
            [sys.executable, "-I", str(SCRIPT), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    # Unprivileged in a temporary directory: on the root file system, or not root's.
    result = run(str(tmp_path / "pulsar"))
    assert result.returncode == 1
    assert result.stdout.startswith("pulsar_storage_path")
    assert run("--docker", str(tmp_path / "pulsar")).returncode == 1
    usage = run()
    assert usage.returncode == 2
    assert usage.stdout == "usage: check_storage.py [--protect] [--docker] PATH\n"


def test_main_succeeds_for_a_volume(volume, monkeypatch, capsys):
    root, info = volume
    monkeypatch.setattr(check_storage, "MOUNTINFO", info)
    monkeypatch.setattr(check_storage, "root_device", lambda: -1)

    assert check_storage.main(["--docker", str(root / "pulsar")]) == 0
    assert "is on an attached storage volume" in capsys.readouterr().out


def test_main_reports_problems_and_usage(volume, monkeypatch, capsys):
    root, info = volume
    monkeypatch.setattr(check_storage, "MOUNTINFO", info)
    monkeypatch.setattr(check_storage, "root_device", lambda: -1)

    assert check_storage.main([str(root)]) == 1
    assert "mount point" in capsys.readouterr().out
    assert check_storage.main(["--docker"]) == 2


def test_root_device_is_that_of_the_root_directory():
    assert check_storage.root_device() == os.stat("/").st_dev


def test_entry_point_exits_with_mains_status(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["check_storage.py"])
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert exit_info.value.code == 2


@pytest.mark.parametrize("mode", [0o775, 0o777])
def test_protect_sets_the_sticky_bit_on_a_shared_volume_root(volume, mode, capsys):
    root, info = volume
    root.chmod(mode)
    assert "chmod +t" in check(root / "pulsar", info)

    assert check_storage.check(str(root / "pulsar"), info, -1, protect_mount=True) is None
    assert stat.S_IMODE(os.stat(root).st_mode) == mode | stat.S_ISVTX
    assert f"set the sticky bit on {root}" in capsys.readouterr().out


def test_protect_leaves_a_private_or_sticky_volume_root_alone(volume, capsys):
    root, info = volume
    for mode in (0o755, 0o1777):
        root.chmod(mode)
        assert check_storage.check(str(root / "pulsar"), info, -1, protect_mount=True) is None
        assert stat.S_IMODE(os.stat(root).st_mode) == mode
    assert capsys.readouterr().out == ""


def test_protect_never_touches_a_refused_volume(tmp_path):
    root = tmp_path / "vol"
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    info = mountinfo(tmp_path, ROOT_FS, (str(root), "tmpfs"))
    assert "tmpfs" in check_storage.check(str(root / "pulsar"), info, -1, protect_mount=True)
    assert stat.S_IMODE(os.stat(root).st_mode) == 0o777


def test_protect_never_touches_the_root_file_system_or_the_path(volume, tmp_path):
    root, info = volume
    root.chmod(0o777)
    (tmp_path / "other").mkdir()
    on_root = mountinfo(tmp_path / "other", ("/", "ext4"))
    assert "root file system" in check_storage.check(str(root / "x"), on_root, protect_mount=True)
    assert "mount point" in check_storage.check(str(root), info, -1, protect_mount=True)
    assert stat.S_IMODE(os.stat(root).st_mode) == 0o777


def test_main_protects_with_the_option(volume, monkeypatch, capsys):
    root, info = volume
    root.chmod(0o777)
    monkeypatch.setattr(check_storage, "MOUNTINFO", info)
    monkeypatch.setattr(check_storage, "root_device", lambda: -1)

    assert check_storage.main(["--docker", "--protect", str(root / "pulsar")]) == 0
    assert "set the sticky bit" in capsys.readouterr().out


def test_a_path_in_no_known_mount_is_on_the_root_file_system(volume, tmp_path):
    root, _ = volume
    empty = tmp_path / "empty"
    empty.write_text("")
    assert "root file system" in check(root / "pulsar", str(empty))
