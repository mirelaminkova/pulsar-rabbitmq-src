"""Galaxy's side of one job on this Pulsar over AMQP, for the end-to-end test.

Does for one small job what Galaxy 26.1's PulsarMQJobRunner (lib/galaxy/jobs/runners/pulsar.py)
does, with Pulsar's client library as Galaxy pins it (pulsar-galaxy-lib, kombu): builds the
client manager from the runner's parameters, consumes status updates and acknowledgements,
lays out the job with the destination's parameters (``rewrite_parameters``: the command
line gets Pulsar's paths from the client's PathMapper, as Galaxy's PulsarComputeEnvironment
does), submits it and waits for the final status, then un-stages it as Galaxy would. The
runner's code is copied here, not vendored: the contract job does not check it.

Not exercised: Galaxy's tool, dependency and container handling, so the destination's
``singularity_enabled``, ``remote_metadata`` and ``outputs_to_working_directory`` have no
effect on this job.

With ``default_file_action: remote_transfer`` Pulsar fetches the inputs from and posts the
outputs to Galaxy's job files API at ``galaxy_url`` (/api/jobs/<id>/files?job_key=..). A small
HTTP server stands in for it, for this job's key only: it serves the files of Galaxy's
temporary directory (datasets and job directory) and accepts uploads only to the job's
output datasets and its job directory.

    python galaxy_job.py CONFIG_JSON [--timeout SECONDS] [--bind ADDRESS]

CONFIG_JSON is ``{"runner": ..., "destination": ...}`` from tests/e2e/galaxy_config.py. Prints
what Galaxy received as JSON; the exit status is 0 when the job completed with exit code 0
and both outputs arrived over HTTP.
"""

import argparse
import email.parser
import email.policy
import json
import secrets
import sys
import tempfile
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from pulsar.client import (
    ClientJobDescription,
    ClientOutputs,
    PathMapper,
    PulsarOutputs,
    build_client_manager,
    finish_job,
    submit_job,
)

# PulsarMQJobRunner.destination_defaults, which _populate_parameter_defaults applies only to
# parameters the destination does not set.
DESTINATION_DEFAULTS = {
    "default_file_action": "remote_transfer",
    "rewrite_parameters": "true",
    "dependency_resolution": "remote",
}
# PulsarJobRunner._pulsar_client_manager_args passes runner parameters with these prefixes.
CLIENT_MANAGER_PREFIXES = ("amqp_", "transport_", "relay_")
FINAL_STATES = ("complete", "failed", "cancelled", "lost")
INPUT_TEXT = "pulsar\ngalaxy\nrabbitmq\n"
# The tool: sorts the input into the first output and reports where it ran into the second.
# Pulsar's monitor publishes "running" when a poll (every min_polling_interval, 0.5 s in
# app.yml) finds the job running; 10 s leaves room for a slow Slurm poll on a busy runner.
RUNNING_SECONDS = 10
COMMAND = (
    "sleep {seconds} && sort {input} > {output} && "
    'echo "user=$(id -un) slurm_job=${{SLURM_JOB_ID:-none}} cwd=$PWD" > {report}'
)


class JobFiles:
    """Galaxy's job files API for one job and its ``job_key``.

    Serves files below ``root``; accepts uploads to the ``outputs`` paths and below
    ``job_dir`` (Pulsar also posts the job's metrics there).
    """

    def __init__(self, root, job_key, job_dir, outputs=()):
        self.root = Path(root).resolve()
        self.job_key = job_key
        self.job_dir = Path(job_dir).resolve()
        self.outputs = {Path(output).resolve() for output in outputs}
        self.received = {}
        self.served = []

    def path(self, query):
        """The requested path, or None for a wrong key or a path outside ``root``."""
        params = parse_qs(query)
        if params.get("job_key") != [self.job_key] or len(params.get("path", [])) != 1:
            return None
        path = Path(params["path"][0]).resolve()
        return path if path.is_relative_to(self.root) else None

    def writable(self, path):
        return path in self.outputs or path.is_relative_to(self.job_dir)


class _Handler(BaseHTTPRequestHandler):
    server: "ThreadingHTTPServer"

    def log_message(self, format, *args):
        pass

    def _path(self):
        url = urlsplit(self.path)
        if not url.path.startswith("/api/jobs/") or not url.path.endswith("/files"):
            return None
        return self.server.job_files.path(url.query)

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _readable(self):
        path = self._path()
        if path is None or not path.is_file():
            self._reply(403 if path is None else 404)
            return None
        return path

    def do_HEAD(self):
        path = self._readable()
        if path is not None:
            self.send_response(200)
            self.send_header("Content-Length", str(path.stat().st_size))
            self.end_headers()

    def do_GET(self):
        path = self._readable()
        if path is not None:
            self.server.job_files.served.append(str(path))
            self._reply(200, path.read_bytes())

    def do_POST(self):
        path = self._path()
        if path is None or not self.server.job_files.writable(path):
            self._reply(403)
            return
        content = self._uploaded_file()
        if content is None:
            self._reply(400)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        self.server.job_files.received[str(path)] = content.decode()
        self._reply(200, b"{}")

    def _uploaded_file(self):
        """The multipart form's one ``file`` field (Pulsar's post_file), or None."""
        length = self.headers["Content-Length"]
        if not (length or "").isdigit():
            return None
        body = self.rfile.read(int(length))
        header = f"Content-Type: {self.headers['Content-Type']}\r\n\r\n".encode()
        message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(header + body)
        if not message.is_multipart():
            return None
        files = [
            part.get_payload(decode=True)
            for part in message.iter_parts()
            if part.get_param("name", header="content-disposition") == "file"
        ]
        return files[0] if len(files) == 1 else None


def serve_job_files(job_files, address):
    """Start the job files API on ``address`` (port 0: any free one) in a thread."""
    server = ThreadingHTTPServer(address, _Handler)
    server.job_files = job_files
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def client_manager_kwargs(runner):
    """PulsarJobRunner._pulsar_client_manager_args for the runner's parameters."""
    kwargs = {"manager": runner.get("manager"), "cache": None, "transport": None}
    kwargs["persistence_directory"] = None
    kwargs.update(
        {key: value for key, value in runner.items() if key.startswith(CLIENT_MANAGER_PREFIXES)}
    )
    return kwargs


class StatusUpdates:
    """PulsarMQJobRunner's status update callback, for one job."""

    def __init__(self, job_id):
        self.job_id = job_id
        self.states = []
        self.final = None
        self.done = threading.Event()

    def __call__(self, full_status):
        if full_status.get("job_id") != self.job_id:
            return
        self.states.append(full_status["status"])
        if full_status["status"] in FINAL_STATES:
            self.final = full_status
            self.done.set()


def run_job(config, timeout=600, root=None, bind=""):
    """Run the job; the job files API listens on ``bind`` and galaxy_url's port.

    With port 0 in galaxy_url, the API takes a free port and galaxy_url gets it.
    """
    runner = config["runner"]
    destination = {**DESTINATION_DEFAULTS, **config["destination"]}
    job_id = f"e2e-{uuid.uuid4().hex}"
    job_key = secrets.token_hex(16)
    with tempfile.TemporaryDirectory(dir=root) as galaxy_dir:
        job_dir = Path(galaxy_dir) / "jobs" / job_id
        working = job_dir / "working"
        objects = Path(galaxy_dir) / "objects"
        tool_dir = Path(galaxy_dir) / "tools"
        for directory in (working, objects, tool_dir):
            directory.mkdir(parents=True)
        dataset_input, dataset_output, report = (objects / f"dataset_{n}.dat" for n in (1, 2, 3))
        dataset_input.write_text(INPUT_TEXT)

        job_files = JobFiles(galaxy_dir, job_key, job_dir, outputs=(dataset_output, report))
        url = urlsplit(runner["galaxy_url"])
        server = serve_job_files(job_files, (bind, url.port))
        galaxy_url = f"{url.scheme}://{url.hostname}:{server.server_port}"
        manager = build_client_manager(**client_manager_kwargs(runner))
        updates = StatusUpdates(job_id)
        try:
            # PulsarMQJobRunner._monitor
            manager.ensure_has_status_update_callback(updates)
            manager.ensure_has_ack_consumers()
            # PulsarJobRunner.get_client
            endpoint = f"{galaxy_url}/api/jobs/{job_id}"
            client = manager.get_client(
                destination,
                job_id=job_id,
                files_endpoint=f"{endpoint}/files?job_key={job_key}",
                token_endpoint=f"{endpoint}/oidc-tokens?job_key={job_key}",
                env=[],
            )
            tool = SimpleNamespace(id="e2e_sort", version="1.0", tool_dir=str(tool_dir))
            remote_job_config = client.setup(tool.id, tool.version, False)
            # rewrite_parameters: Galaxy builds the command line with Pulsar's paths.
            paths = PathMapper(client, remote_job_config, str(working))
            command_line = COMMAND.format(
                seconds=RUNNING_SECONDS,
                input=paths.remote_input_path_rewrite(str(dataset_input)),
                output=paths.remote_output_path_rewrite(str(dataset_output)),
                report=paths.remote_output_path_rewrite(str(report)),
            )
            client_outputs = ClientOutputs(
                working_directory=str(working),
                metadata_directory=str(job_dir),
                job_directory=str(job_dir),
                output_files=[str(dataset_output), str(report)],
            )
            description = ClientJobDescription(
                command_line=command_line,
                tool=tool,
                input_files=[str(dataset_input)],
                client_outputs=client_outputs,
                working_directory=str(working),
                metadata_directory=str(job_dir),
                env=client.env,
                rewrite_paths=False,
            )
            submit_job(client, description, remote_job_config)
            if not updates.done.wait(timeout):
                client.kill()
            final = updates.final or {}
            collection_failures = []
            if final.get("status") == "complete":
                collection_failures = finish_job(
                    client=client,
                    cleanup_job="never",
                    job_completed_normally=True,
                    client_outputs=client_outputs,
                    pulsar_outputs=PulsarOutputs.from_status_response(final),
                )
            received = {
                "output": job_files.received.get(str(dataset_output.resolve())),
                "report": job_files.received.get(str(report.resolve())),
            }
        finally:
            manager.shutdown()
            server.shutdown()
            server.server_close()
    fields = (received["report"] or "").split()
    return {
        "job_id": job_id,
        "states": updates.states,
        "status": final.get("status", "timeout"),
        "returncode": final.get("returncode"),
        "stderr": final.get("stderr"),
        "remote_job_config": remote_job_config,
        "inputs_served": len(job_files.served),
        "collection_failures": [str(failure) for failure in collection_failures],
        "received": received,
        # What the job reported about itself: user, slurm_job and cwd.
        "report": dict(field.split("=", 1) for field in fields if "=" in field),
    }


def succeeded(result):
    return (
        result["status"] == "complete"
        and result["returncode"] == 0
        and not result["collection_failures"]
        and result["received"]["output"] == "".join(sorted(INPUT_TEXT.splitlines(True)))
        and result["received"]["report"] is not None
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("config")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--bind", default="", help="address of the job files API (default: all)")
    args = parser.parse_args(argv)
    config = json.loads(Path(args.config).read_text())
    result = run_job(config, timeout=args.timeout, bind=args.bind)
    print(json.dumps(result, indent=2))
    return 0 if succeeded(result) else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
