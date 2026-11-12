"""tests/live/pulsar_job.py, the job CI's Slurm live check runs, here with the local runner.

It also shows what the Docker epilog relies on: a job's working directory is
<staging>/<job>/working and Galaxy's container_config.json sits in ../configs.
"""

import getpass
import json
import time
from pathlib import Path

import pytest

from tests.helpers import render
from tests.live import pulsar_job


@pytest.fixture
def app_yml(tmp_path):
    (tmp_path / "staging").mkdir()
    (tmp_path / "job_metrics_conf.xml").write_text("<job_metrics><core /></job_metrics>")
    text = render(
        "pulsar-app.yml.j2",
        pulsar_job_runner="local",
        pulsar_storage_path="/data/live/test",
        pulsar_config_dir=str(tmp_path),
    ).replace("/data/live/test", str(tmp_path))
    path = tmp_path / "app.yml"
    path.write_text(text)
    return path


def test_runs_a_job_through_the_manager_without_a_broker(app_yml, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    result = pulsar_job.run_job(str(app_yml), container="galaxy_live-1", timeout=60, poll=0.1)

    assert (result["manager"], result["manager_type"]) == ("production", "queued_python")
    assert (result["status"], result["return_code"]) == ("complete", 0)
    assert result["user"] == getpass.getuser()
    assert result["slurm_job"] == "none"
    # Where the job's temporary files and Apptainer images go: under Slurm, the storage
    # (task prolog); Pulsar's own jobs get a TMPDIR and drop APPTAINER_CACHEDIR.
    assert result["tmp"] not in ("", "none")
    assert result["apptainer_cache"] == "none"
    working = Path(result["cwd"])
    assert working.name == "working"
    assert working.parent.parent == tmp_path / "staging"
    config = working.parent / "configs" / "container_config.json"
    assert json.loads(config.read_text()) == {"container_name": "galaxy_live-1"}


def test_app_conf_leaves_the_broker_out(app_yml):
    conf = pulsar_job.app_conf(str(app_yml), "/elsewhere")

    assert "message_queue_url" not in conf
    assert not [key for key in conf if key.startswith("amqp_")]
    assert conf["persistence_directory"] == "/elsewhere"
    assert conf["managers"]["production"]["type"] == "queued_python"


def test_main_reports_the_result(app_yml, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)

    assert pulsar_job.main([str(app_yml), "--timeout", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "complete"


def test_main_fails_for_a_failed_job(app_yml, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    assert pulsar_job.main([str(app_yml), "--command", "exit 3", "--timeout", "60"]) == 1


def test_command_runs_first(app_yml, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    result = pulsar_job.run_job(str(app_yml), command="echo first=yes", timeout=60, poll=0.1)

    assert result["stdout"].startswith("first=yes\n")
    assert result["return_code"] == 0


def test_a_job_past_its_timeout_is_killed(app_yml, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    start = time.monotonic()

    result = pulsar_job.run_job(str(app_yml), command="sleep 30", timeout=1, poll=0.2)

    assert time.monotonic() - start < 20
    assert result["return_code"] != 0
    assert "user" not in result  # the job never got past its first command


def test_require_slurm_passes_for_a_slurm_job(monkeypatch):
    def slurm_job(*_args, **_kwargs):
        return {"status": "complete", "return_code": 0, "slurm_job": "123"}

    monkeypatch.setattr(pulsar_job, "run_job", slurm_job)

    assert pulsar_job.main(["app.yml", "--require-slurm"]) == 0


def test_require_slurm_fails_outside_slurm(app_yml, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    assert pulsar_job.main([str(app_yml), "--require-slurm", "--timeout", "60"]) == 1
