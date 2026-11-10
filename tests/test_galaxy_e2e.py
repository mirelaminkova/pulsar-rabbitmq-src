"""The end-to-end test's Galaxy side (tests/e2e/), here against an in-process Pulsar.

CI's end-to-end job runs tests/e2e/galaxy_job.py against the deployed Pulsar, through the
test RabbitMQ and Slurm. Here the same client talks to a Pulsar app in this process over
kombu's in-memory transport, configured from our app.yml with the local runner, so the
staging over HTTP, the status updates and the un-staging are tested without a broker.
"""

import http.client
import json
import socket
import uuid

import pytest
import yaml
from pulsar.core import PulsarApp

from tests.e2e import galaxy_config, galaxy_job
from tests.helpers import PASSWORD, render, resolve

GALAXY_PASSWORD = "Galaxy.Pass_word~456"  # noqa: S105 - test value


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def entry():
    return resolve("_pulsar_amqp")["galaxy_entry"]


def test_runner_and_destination_are_galaxys(entry):
    config = galaxy_config.galaxy_job_config(entry, GALAXY_PASSWORD, "https://galaxy.example.org")

    assert config["runner"] == {
        "load": "galaxy.jobs.runners.pulsar:PulsarMQJobRunner",
        # Galaxy's own user, over plain AMQP on its loopback listener, on our vhost.
        "amqp_url": f"pyamqp://galaxy:{GALAXY_PASSWORD}@127.0.0.1:5672//pulsar/hpc",
        "galaxy_url": "https://galaxy.example.org",
        "manager": "_default_",
        "amqp_acknowledge": True,
        "amqp_ack_republish_time": 1200,
        "amqp_consumer_timeout": 2.0,
        "amqp_publish_retry": True,
        "amqp_publish_retry_max_retries": 60,
    }
    destination = config["destination"]
    # Galaxy stages into our storage's staging directory, Pulsar's staging_directory.
    assert destination["jobs_directory"] == "/data/pulsar_storage/pulsar/staging"
    assert destination["jobs_directory"] == render_app_yml()["staging_directory"]
    assert destination["default_file_action"] == "remote_transfer"
    assert destination["singularity_enabled"] is True
    assert destination["docker_enabled"] is False


def render_app_yml(**params):
    return yaml.safe_load(render("pulsar-app.yml.j2", **params))


def test_an_invalid_entry_is_refused():
    with pytest.raises(ValueError, match="'name' is required"):
        galaxy_config.galaxy_job_config("[{password: x}]", GALAXY_PASSWORD, "http://g.test")


def test_config_command_line(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("GALAXY_AMQP_PASSWORD", GALAXY_PASSWORD)
    params = tmp_path / "params.yml"
    params.write_text("pulsar_amqp_name: molecule\npulsar_storage_path: /data/molecule/pulsar\n")

    galaxy_config.main([str(params), "http://galaxy.test:8080"])

    config = json.loads(capsys.readouterr().out)
    assert config["runner"]["galaxy_url"] == "http://galaxy.test:8080"
    assert config["runner"]["amqp_url"].endswith("@127.0.0.1:5672//pulsar/molecule")
    assert config["destination"]["jobs_directory"] == "/data/molecule/pulsar/staging"
    assert PASSWORD not in json.dumps(config)


def test_client_manager_gets_galaxys_amqp_parameters():
    runner = {"manager": "gpu", "amqp_url": "pyamqp://", "galaxy_url": "http://g", "load": "x"}
    runner |= {"amqp_acknowledge": True, "transport_timeout": 5}

    kwargs = galaxy_job.client_manager_kwargs(runner)

    assert kwargs == {
        "manager": "gpu",
        "cache": None,
        "transport": None,
        "persistence_directory": None,
        "amqp_url": "pyamqp://",
        "amqp_acknowledge": True,
        "transport_timeout": 5,
    }


@pytest.fixture
def pulsar(tmp_path, monkeypatch):
    """Our app.yml (local runner) on an in-memory broker, as a running Pulsar app."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "staging").mkdir()
    (tmp_path / "persisted").mkdir()
    (tmp_path / "job_metrics_conf.xml").write_text("<job_metrics><core /></job_metrics>")
    conf = render_app_yml(pulsar_job_runner="local", pulsar_config_dir=str(tmp_path))
    url = f"memory://e2e-{uuid.uuid4().hex}"
    conf = {key: value for key, value in conf.items() if not key.startswith("amqp_connect_ssl")}
    conf |= {
        "message_queue_url": url,
        "staging_directory": str(tmp_path / "staging"),
        "persistence_directory": str(tmp_path / "persisted"),
        # Faster retries of a failed publish than the deployment's.
        "amqp_publish_retry_interval_start": 1,
        "amqp_publish_retry_interval_step": 1,
    }
    app = PulsarApp(**conf)
    yield {"url": url, "staging": tmp_path / "staging"}
    app.shutdown(timeout=5)


def config_for(pulsar, entry):
    config = galaxy_config.galaxy_job_config(
        entry, GALAXY_PASSWORD, f"http://127.0.0.1:{free_port()}"
    )
    config["runner"]["amqp_url"] = pulsar["url"]
    config["destination"]["jobs_directory"] = str(pulsar["staging"])
    return config


def test_a_job_runs_and_its_outputs_come_back_over_http(pulsar, entry, tmp_path):
    result = galaxy_job.run_job(config_for(pulsar, entry), timeout=60, root=tmp_path)

    assert (result["status"], result["returncode"]) == ("complete", 0), result
    assert galaxy_job.succeeded(result)
    # Status updates arrived before the final one.
    assert "running" in result["states"]
    assert result["states"][-1] == "complete"
    # Pulsar fetched the input over HTTP and ran the job in its staging directory.
    assert result["inputs_served"] >= 1
    assert result["cwd"].startswith(str(pulsar["staging"]))
    assert result["slurm_job"] == "none"
    assert result["received"]["output"] == "galaxy\npulsar\nrabbitmq\n"


def test_main_reports_a_failed_job(pulsar, entry, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(galaxy_job, "COMMAND", "exit 3; {input} {output} {report}")
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config_for(pulsar, entry)))

    assert galaxy_job.main([str(path), "--timeout", "60"]) == 1

    result = json.loads(capsys.readouterr().out)
    assert (result["status"], result["returncode"]) == ("complete", 3)
    assert result["received"]["report"] is None


def test_a_job_without_a_final_status_times_out(pulsar, entry, tmp_path):
    config = config_for(pulsar, entry)
    # No Pulsar consumes this manager's queues.
    config["runner"]["manager"] = "nobody"

    result = galaxy_job.run_job(config, timeout=1, root=tmp_path)

    assert result["status"] == "timeout"
    assert not galaxy_job.succeeded(result)


def test_status_updates_of_other_jobs_are_ignored():
    updates = galaxy_job.StatusUpdates("mine")

    updates({"job_id": "other", "status": "complete"})
    updates({"job_id": "mine", "status": "running"})

    assert updates.states == ["running"]
    assert not updates.done.is_set()


@pytest.fixture
def job_files(tmp_path):
    (tmp_path / "job").mkdir()
    (tmp_path / "job" / "in.dat").write_text("data")
    files = galaxy_job.JobFiles(tmp_path / "job", "key1")
    port = free_port()
    server = galaxy_job.serve_job_files(files, f"http://127.0.0.1:{port}")
    yield files, port, tmp_path
    server.shutdown()
    server.server_close()


def request(port, method, target, body=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, target, body=body)
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


API = "/api/jobs/1/files"


@pytest.mark.parametrize(
    ("query", "code"),
    [
        pytest.param("job_key=wrong&path={job}/in.dat", 403, id="wrong-key"),
        pytest.param("path={job}/in.dat", 403, id="no-key"),
        pytest.param("job_key=key1&path={outside}", 403, id="outside-the-job"),
        pytest.param("job_key=key1&path={job}/../job/../secret", 403, id="dot-dot"),
        pytest.param("job_key=key1&path={job}/missing", 404, id="missing"),
    ],
)
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_job_files_refuses(job_files, query, code, method):
    _, port, root = job_files
    query = query.format(job=root / "job", outside=root / "secret")

    assert request(port, method, f"{API}?{query}")[0] == code


def test_job_files_serves_files_of_the_job(job_files):
    files, port, root = job_files
    target = f"{API}?job_key=key1&path={root / 'job' / 'in.dat'}"

    status, headers, _ = request(port, "HEAD", target)
    assert (status, headers["Content-Length"]) == (200, "4")
    assert request(port, "GET", target)[::2] == (200, b"data")
    assert files.served == [str(root / "job" / "in.dat")]


@pytest.mark.parametrize(
    "target", ["/other", f"{API}?job_key=wrong&path=/x"], ids=["not-the-api", "wrong-key"]
)
def test_job_files_refuses_uploads(job_files, target):
    _, port, _ = job_files

    assert request(port, "POST", target, body=b"x")[0] == 403
