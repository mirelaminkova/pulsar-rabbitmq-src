"""The end-to-end test's Galaxy side (tests/e2e/), here against an in-process Pulsar.

CI's end-to-end job runs tests/e2e/galaxy_job.py against the deployed Pulsar, through the
test RabbitMQ and Slurm. Here the same client talks to a Pulsar app in this process over
kombu's in-memory transport, configured from our app.yml with the local runner, so the
staging over HTTP, the status updates and the un-staging are tested without a broker.
"""

import http.client
import json
import uuid

import pytest
import yaml
from pulsar.core import PulsarApp

from tests.e2e import galaxy_config, galaxy_job
from tests.helpers import PASSWORD, URL, render


def render_app_yml(**params):
    return yaml.safe_load(render("pulsar-app.yml.j2", **params))


@pytest.fixture
def params():
    return {}


def test_runner_and_destination_are_byoc_syncs():
    config = galaxy_config.galaxy_job_config({}, "https://usegalaxy.eu")

    assert config["runner"] == {
        "load": "galaxy.jobs.runners.pulsar:PulsarMQJobRunner",
        # Galaxy connects as this Pulsar's own user; ?ssl=1 encrypts without verification
        # on the Galaxy side (Pulsar's side verifies, test_pulsar_compat).
        "amqp_url": f"{URL}?ssl=1",
        "galaxy_url": "https://usegalaxy.eu",
        "manager": "production",
        "amqp_acknowledge": "true",
        "amqp_ack_republish_time": 300,
        "amqp_consumer_timeout": 2.0,
        "amqp_publish_retry": "true",
        "amqp_publish_retry_max_retries": 60,
    }
    destination = config["destination"]
    # Galaxy stages into our storage's staging directory, Pulsar's staging_directory.
    assert destination["jobs_directory"] == "/data/pulsar_storage/pulsar/staging"
    assert destination["jobs_directory"] == render_app_yml()["staging_directory"]
    assert destination["persistence_directory"] == render_app_yml()["persistence_directory"]
    assert set(destination) == {"jobs_directory", "persistence_directory"}


def test_invalid_parameters_are_refused():
    with pytest.raises(ValueError, match="pulsar_amqp_name"):
        galaxy_config.galaxy_job_config({"pulsar_amqp_name": "hpc_byoc"}, "http://g.test")


def test_empty_credentials_are_refused():
    # The playbook generates them; the end-to-end test needs the ones it used.
    with pytest.raises(ValueError, match="pulsar_amqp_name and pulsar_amqp_password"):
        galaxy_config.galaxy_job_config({"pulsar_amqp_password": ""}, "http://g.test")


def test_config_command_line(capsys, tmp_path):
    params = tmp_path / "params.yml"
    params.write_text(
        "pulsar_amqp_host: galaxy.molecule.test\npulsar_amqp_name: molecule\n"
        "pulsar_amqp_password: MoleculePulsar1234\npulsar_storage_path: /data/molecule/pulsar\n"
    )

    galaxy_config.main([str(params), "http://galaxy.test:8080"])

    config = json.loads(capsys.readouterr().out)
    assert config["runner"]["galaxy_url"] == "http://galaxy.test:8080"
    assert config["runner"]["amqp_url"] == (
        "pyamqp://galaxy_molecule_byoc:MoleculePulsar1234@galaxy.molecule.test:5671"
        "//pulsar/galaxy_molecule_byoc?ssl=1"
    )
    assert config["destination"]["jobs_directory"] == "/data/molecule/pulsar/staging"
    assert PASSWORD not in json.dumps(config)


def test_client_manager_gets_galaxys_amqp_parameters():
    runner = {"manager": "gpu", "amqp_url": "pyamqp://", "galaxy_url": "http://g", "load": "x"}
    runner |= {"amqp_acknowledge": True, "transport_timeout": 5, "relay_url": None}

    kwargs = galaxy_job.client_manager_kwargs(runner)

    assert kwargs == {
        "manager": "gpu",
        "cache": None,
        "transport": None,
        "persistence_directory": None,
        "amqp_url": "pyamqp://",
        "amqp_acknowledge": True,
        "transport_timeout": 5,
        "relay_url": None,
    }


@pytest.fixture
def pulsar(tmp_path, monkeypatch):
    """Our app.yml (local runner) on an in-memory broker, as a running Pulsar app."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "staging").mkdir()
    (tmp_path / "persisted").mkdir()
    (tmp_path / "job_metrics_conf.xml").write_text("<job_metrics><core /></job_metrics>")
    conf = render_app_yml(pulsar_job_runner="local", pulsar_config_dir=str(tmp_path))
    # kombu's memory transport has one broker per process, whatever the URL: the tests stay
    # apart through their own job ids (StatusUpdates ignores other jobs).
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


def config_for(pulsar, params):
    # Port 0: the job files API takes a free port.
    config = galaxy_config.galaxy_job_config(params, "http://127.0.0.1:0")
    config["runner"]["amqp_url"] = pulsar["url"]
    config["destination"]["jobs_directory"] = str(pulsar["staging"])
    return config


def test_a_job_runs_and_its_outputs_come_back_over_http(pulsar, params, tmp_path, monkeypatch):
    # The local runner polls every 0.5 s; the 10 s are for Slurm.
    monkeypatch.setattr(galaxy_job, "RUNNING_SECONDS", 2)
    result = galaxy_job.run_job(
        config_for(pulsar, params), timeout=60, root=tmp_path, bind="127.0.0.1"
    )

    assert (result["status"], result["returncode"]) == ("complete", 0), result
    assert galaxy_job.succeeded(result)
    # Status updates arrived before the final one.
    assert "running" in result["states"]
    assert result["states"][-1] == "complete"
    # Pulsar fetched the input over HTTP and ran the job in its staging directory.
    assert result["inputs_served"] >= 1
    assert result["report"]["cwd"].startswith(str(pulsar["staging"]))
    assert result["report"]["slurm_job"] == "none"
    assert result["received"]["output"] == "galaxy\npulsar\nrabbitmq\n"


def test_main_reports_a_failed_job(pulsar, params, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(galaxy_job, "COMMAND", "exit 3; {input} {output} {report}")
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config_for(pulsar, params)))

    assert galaxy_job.main([str(path), "--timeout", "60", "--bind", "127.0.0.1"]) == 1

    result = json.loads(capsys.readouterr().out)
    assert (result["status"], result["returncode"]) == ("complete", 3)
    assert result["received"]["report"] is None
    assert result["report"] == {}


def test_a_job_without_a_final_status_times_out(pulsar, params, tmp_path):
    config = config_for(pulsar, params)
    # No Pulsar consumes this manager's queues.
    config["runner"]["manager"] = "nobody"

    result = galaxy_job.run_job(config, timeout=1, root=tmp_path, bind="127.0.0.1")

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
    files = galaxy_job.JobFiles(
        tmp_path, "key1", job_dir=tmp_path / "job" / "dir", outputs=[tmp_path / "out.dat"]
    )
    server = galaxy_job.serve_job_files(files, ("127.0.0.1", 0))
    yield files, server.server_port, tmp_path
    server.shutdown()
    server.server_close()


def request(port, method, target, body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, target, body=body, headers=headers or {})
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
        pytest.param("job_key=key1&path={job}/../job/../../secret", 403, id="dot-dot"),
        pytest.param("job_key=key1&path={job}/missing", 404, id="missing"),
    ],
)
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_job_files_refuses(job_files, query, code, method):
    _, port, root = job_files
    query = query.format(job=root / "job", outside=root.parent / "secret")

    assert request(port, method, f"{API}?{query}")[0] == code


def test_job_files_serves_files_of_the_job(job_files):
    files, port, root = job_files
    target = f"{API}?job_key=key1&path={root / 'job' / 'in.dat'}"

    status, headers, _ = request(port, "HEAD", target)
    assert (status, headers["Content-Length"]) == (200, "4")
    assert request(port, "GET", target)[::2] == (200, b"data")
    assert files.served == [str(root / "job" / "in.dat")]


def multipart(*fields):
    boundary = "e2e-boundary"
    parts = [
        f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; filename="x"\r\n\r\n'
        f"{value}\r\n"
        for name, value in fields
    ]
    body = ("".join(parts) + f"--{boundary}--\r\n").encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={boundary}"}


@pytest.mark.parametrize("path", ["out.dat", "job/dir/metadata/x"], ids=["output", "job-dir"])
def test_job_files_accepts_outputs_and_job_directory_files(job_files, path):
    files, port, root = job_files
    body, headers = multipart(("file", "result"))

    status = request(port, "POST", f"{API}?job_key=key1&path={root / path}", body, headers)[0]

    assert status == 200
    assert (root / path).read_text() == "result"
    assert files.received == {str(root / path): "result"}


@pytest.mark.parametrize(
    "target",
    ["/other", f"{API}?job_key=wrong&path=/x", f"{API}?job_key=key1&path={{root}}/job/in.dat"],
    ids=["not-the-api", "wrong-key", "an-input"],
)
def test_job_files_refuses_uploads(job_files, target):
    _, port, root = job_files
    body, headers = multipart(("file", "overwritten"))

    assert request(port, "POST", target.format(root=root), body, headers)[0] == 403
    assert (root / "job" / "in.dat").read_text() == "data"


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        pytest.param(b"plain", {"Content-Type": "text/plain"}, id="not-multipart"),
        pytest.param(*multipart(("other", "x")), id="no-file-field"),
        pytest.param(*multipart(("file", "a"), ("file", "b")), id="two-files"),
    ],
)
def test_job_files_rejects_bad_uploads(job_files, body, headers):
    _, port, root = job_files
    target = f"{API}?job_key=key1&path={root / 'out.dat'}"

    assert request(port, "POST", target, body, headers)[0] == 400
    assert not (root / "out.dat").exists()


def test_job_files_rejects_an_upload_without_length(job_files):
    _, port, root = job_files
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.putrequest("POST", f"{API}?job_key=key1&path={root / 'out.dat'}")
        connection.endheaders()
        assert connection.getresponse().status == 400
    finally:
        connection.close()
