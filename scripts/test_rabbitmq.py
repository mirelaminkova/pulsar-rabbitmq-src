#!/usr/bin/env python3
"""Live test: start RabbitMQ from compose.yml and exchange Pulsar messages through it.

Runs a disposable Compose project on free localhost ports, then, with Pulsar's own
PulsarExchange (as both Galaxy's PulsarMQJobRunner and Pulsar use it):
  * the pair account publishes a setup message *before* any consumer exists and
    Pulsar's side receives it (queues are pre-declared by the definitions);
  * Pulsar's side publishes a status update that Galaxy's side receives;
  * the management API works for the admin account, and the pair account and
    the default guest account cannot use it;
  * messages survive recreation of the container (durable queues, named volume).

Needs Docker with Compose v2 (through sudo unless --no-sudo) and pulsar-app[amqp]
importable. Removes only its own project and volume unless --keep is given.
"""
import argparse
import base64
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from configure import configure  # noqa: E402

from pulsar.client.amqp_exchange_factory import get_exchange  # noqa: E402


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def api(port, path, user, password):
    request = urllib.request.Request(f"http://127.0.0.1:{port}/rabbitmq/api/{path}")
    request.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode())
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None


def receive_one(exchange, queue, timeout=30):
    """Consume one message from `queue` with PulsarExchange.consume, then stop."""
    received = []

    class Check:
        def __bool__(self):
            return not received

    def callback(body, message):
        received.append(body)
        message.ack()

    deadline = time.time() + timeout
    import threading
    thread = threading.Thread(target=exchange.consume, args=(queue, callback), kwargs={"check": Check()},
                              daemon=True)
    thread.start()
    while not received and time.time() < deadline:
        time.sleep(0.2)
    if not received:
        raise AssertionError(f"no message on {queue} within {timeout}s")
    return received[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keep", action="store_true", help="keep the test project for inspection")
    parser.add_argument("--no-sudo", action="store_true", help="call docker directly")
    args = parser.parse_args()
    docker = ["docker"] if args.no_sudo else ["sudo", "docker"]
    project = "pulsar-rabbitmq-test-" + uuid.uuid4().hex[:8]
    workdir = Path(tempfile.mkdtemp(prefix="pulsar-rabbitmq-test-"))
    shutil.copy(ROOT / "compose.yml", workdir / "compose.yml")
    amqp_port, management_port = free_port(), free_port()
    configure(workdir, amqp_port=amqp_port, management_port=management_port)
    secrets = json.loads((workdir / "secrets.json").read_text())
    pair, admin = secrets["RABBITMQ_PAIR_PASSWORD"], secrets["RABBITMQ_ADMIN_PASSWORD"]
    compose = docker + ["compose", "-p", project, "--project-directory", str(workdir)]
    url = f"amqp://galaxy-pulsar:{pair}@127.0.0.1:{amqp_port}/pulsar"

    def up():
        subprocess.run(compose + ["up", "-d", "--wait", "--wait-timeout", "240"], check=True)
        for _ in range(60):
            if api(management_port, "permissions/pulsar/galaxy-pulsar", "admin", admin)[0] == 200:
                return
            time.sleep(2)
        raise AssertionError("definitions were not imported")

    try:
        up()
        print("ok: broker up, definitions imported")

        status, queues = api(management_port, "queues/pulsar", "admin", admin)
        names = {queue["name"] for queue in queues}
        assert {"pulsar__setup", "pulsar__kill", "pulsar__status", "pulsar__status_update"} <= names, names
        print("ok: Pulsar queues pre-declared:", ", ".join(sorted(names)))
        assert api(management_port, "overview", "galaxy-pulsar", pair)[0] == 401
        assert api(management_port, "overview", "guest", "guest")[0] == 401
        print("ok: pair and guest accounts have no management access")

        # Galaxy's side (PulsarMQJobRunner with manager _default_) and Pulsar's side.
        galaxy = get_exchange(url, "_default_", {})
        pulsar = get_exchange(url, "_default_", {})

        job = {"job_id": "test-" + uuid.uuid4().hex[:6], "setup_params": {"x": 1}}
        galaxy.publish("setup", dict(job))  # no consumer connected yet
        assert receive_one(pulsar, "setup")["job_id"] == job["job_id"]
        print("ok: Galaxy -> Pulsar setup message delivered (published before Pulsar connected)")

        pulsar.publish("status_update", {"job_id": job["job_id"], "status": "complete"})
        assert receive_one(galaxy, "status_update")["status"] == "complete"
        print("ok: Pulsar -> Galaxy status update delivered")

        galaxy.publish("kill", {"job_id": "persist-me"})
        subprocess.run(compose + ["up", "-d", "--force-recreate", "--wait", "--wait-timeout", "240"], check=True)
        up()
        assert receive_one(get_exchange(url, "_default_", {}), "kill")["job_id"] == "persist-me"
        print("ok: queued message survived container recreation")
        print("PASS")
    finally:
        if args.keep:
            print(f"kept project {project} in {workdir}")
        else:
            subprocess.run(compose + ["down", "--volumes", "--remove-orphans"], check=False)
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
