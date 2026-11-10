"""files/epilog.sh with stand-ins for scontrol, runuser, jq and docker.

Slurm runs the epilog as root after every job on the node, also a cancelled one.
The CI job `slurm-live` runs it for real, with Docker.
"""

import json
import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

from tests.helpers import REPO

EPILOG = REPO / "files" / "epilog.sh"

FAKES = {
    # WorkDir as `scontrol show job` prints it, among other fields.
    "scontrol": '#!/bin/bash\necho "   UserId=pulsar(999) WorkDir=$FAKE_WORKDIR StdErr=/x"\n',
    # runuser -u USER -- COMMAND...: record the user, run the command.
    "runuser": '#!/bin/bash\necho "$2" >> "$FAKE_LOG.runuser"\nshift 3\nexec "$@"\n',
    "jq": (
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "assert sys.argv[1:3] == ['-r', '.container_name // empty'], sys.argv\n"
        "value = json.load(open(sys.argv[3])).get('container_name')\n"
        "print('' if value is None else value)\n"
    ),
    "docker": '#!/bin/bash\necho "$@" >> "$FAKE_LOG.docker"\nexit "${FAKE_DOCKER_EXIT:-0}"\n',
}


@pytest.fixture
def epilog(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, text in FAKES.items():
        path = bin_dir / name
        path.write_text(text)
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    # Slurm gives the epilog no search path, so the script sets one; put the
    # stand-ins in front of it.
    script = tmp_path / "epilog.sh"
    text, count = re.subn(
        r"^export PATH=(.*)$", rf"export PATH={bin_dir}:\1", EPILOG.read_text(), flags=re.M
    )
    assert count == 1
    script.write_text(text)
    job = tmp_path / "staging" / "42"
    (job / "working").mkdir(parents=True)
    (job / "configs").mkdir()
    log = tmp_path / "log"

    def run(env=None, job_id="7"):
        environ = {
            "FAKE_WORKDIR": str(job / "working"),
            "FAKE_LOG": str(log),
            "SLURM_JOB_USER": "pulsar",
            **(env or {}),
        }
        if job_id is not None:
            environ["SLURM_JOB_ID"] = job_id
        result = subprocess.run(  # noqa: S603 - our own script
            ["/bin/bash", str(script)], env=environ, capture_output=True, text=True, check=False
        )
        calls = {
            name: Path(f"{log}.{name}").read_text().splitlines()
            if Path(f"{log}.{name}").exists()
            else []
            for name in ("docker", "runuser")
        }
        return result, calls

    run.configs = job / "configs"
    return run


def write_config(epilog, name):
    (epilog.configs / "container_config.json").write_text(json.dumps({"container_name": name}))


def test_kills_the_jobs_container(epilog):
    write_config(epilog, "galaxy_7f3a-b.c_1")

    result, calls = epilog()

    assert result.returncode == 0
    assert result.stdout == result.stderr == ""
    assert calls["docker"] == ["kill galaxy_7f3a-b.c_1"]
    # The job's file is read with the job user's rights, not root's.
    assert calls["runuser"] == ["pulsar"]


def test_without_a_container_config_nothing_happens(epilog):
    result, calls = epilog()

    assert result.returncode == 0
    assert calls == {"docker": [], "runuser": []}


def test_without_a_job_id_nothing_happens(epilog):
    write_config(epilog, "galaxy_1")

    result, calls = epilog(job_id=None)

    assert result.returncode == 0
    assert calls["docker"] == []


@pytest.mark.parametrize("name", ["-t", "--help", "a b", "a;b", "$(id)", "", "x" * 129, None])
def test_only_plain_container_names_are_killed(epilog, name):
    # The job (as pulsar) writes this file; a name must not become a docker option.
    write_config(epilog, name)

    result, calls = epilog()

    assert result.returncode == 0
    assert calls["docker"] == []


def test_a_link_is_not_followed(epilog, tmp_path):
    target = tmp_path / "elsewhere.json"
    target.write_text(json.dumps({"container_name": "galaxy_1"}))
    (epilog.configs / "container_config.json").symlink_to(target)

    _, calls = epilog()

    assert calls["docker"] == []


def test_a_fifo_is_not_read(epilog):
    # Reading a FIFO would block until PrologEpilogTimeout and drain the node.
    os.mkfifo(epilog.configs / "container_config.json")

    result, calls = epilog()

    assert result.returncode == 0
    assert calls == {"docker": [], "runuser": []}


def test_failures_never_fail_the_epilog(epilog):
    # A failing epilog drains the node: no job could start any more.
    write_config(epilog, "galaxy_1")

    result, calls = epilog(env={"FAKE_DOCKER_EXIT": "1"})

    assert result.returncode == 0
    assert calls["docker"] == ["kill galaxy_1"]


def test_bad_json_never_fails_the_epilog(epilog):
    (epilog.configs / "container_config.json").write_text("{not json")

    result, calls = epilog()

    assert result.returncode == 0
    assert calls["docker"] == []


def test_epilog_sets_its_own_path():
    # Slurm runs the epilog without a search path.
    text = EPILOG.read_text()
    assert len(re.findall(r"^export PATH=/usr/sbin:/usr/bin:/sbin:/bin$", text, re.M)) == 1


def test_commands_are_time_limited():
    # Only the source: the stand-ins above do not hang.
    # PrologEpilogTimeout is 90 s; a hanging command must not reach it.
    text = EPILOG.read_text()
    assert re.search(r"timeout 15 scontrol ", text)
    assert re.search(r"timeout 10 jq ", text)
    assert re.search(r"timeout 30 docker kill ", text)
