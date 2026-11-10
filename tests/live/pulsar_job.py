"""Run one job through the manager in Pulsar's app.yml, as Pulsar would, and report it.

Used by CI's live Slurm check (as the pulsar user, with the deployed venv, app.yml and
DRMAA_LIBRARY_PATH), and by the unit tests with the local runner. Pulsar's app is
built from app.yml without its broker, so no Galaxy is needed: the job goes from
Pulsar's manager (queued_drmaa: through libdrmaa to Slurm) to a result.

    python pulsar_job.py APP_YML [--container NAME] [--command CMD] [--require-slurm]
                         [--timeout SECONDS]

With --container, the job writes NAME as Galaxy's container_config.json, so that
Slurm's epilog stops that Docker container when the job ends. Prints the job's
result as JSON; the exit status is 0 when the job ran and succeeded. --command
runs CMD in the job first (a container, say), and the job fails when CMD fails.
--require-slurm also fails when the job did not run as a Slurm job.
"""

import argparse
import json
import sys
import tempfile
import time
import uuid

import yaml
from pulsar.core import PulsarApp
from pulsar.managers import status

# What the job reports about itself: user, Slurm job, working directory.
COMMAND = 'echo "user=$(id -un) slurm_job=${SLURM_JOB_ID:-none} cwd=$PWD"'
CONTAINER_CONFIG = 'printf \'{"container_name": "%s"}\' {name} > ../configs/container_config.json'


def app_conf(app_yml, persistence_directory):
    """app.yml without the broker, with its own persistence directory."""
    with open(app_yml) as handle:
        conf = yaml.safe_load(handle)
    conf = {
        key: value
        for key, value in conf.items()
        if key != "message_queue_url" and not key.startswith("amqp_")
    }
    conf["persistence_directory"] = persistence_directory
    return conf


def run_job(app_yml, container=None, timeout=300, poll=1.0, command=None):
    with tempfile.TemporaryDirectory() as persistence_directory:
        app = PulsarApp(**app_conf(app_yml, persistence_directory))
        try:
            [manager_name] = app.managers
            manager = app.managers[manager_name]._proxied_manager
            job_id = manager.setup_job(f"live-{uuid.uuid4().hex}", "live_check", "1.0")
            commands = [COMMAND]
            if command:
                commands.insert(0, command)
            if container:
                commands.insert(0, CONTAINER_CONFIG.replace("{name}", container))
            command = " && ".join(commands)
            manager.launch(job_id, command)
            deadline = time.monotonic() + timeout
            state = manager.get_status(job_id)
            while state not in (status.COMPLETE, status.FAILED, status.LOST):
                if time.monotonic() > deadline:
                    manager.kill(job_id)
                    break
                time.sleep(poll)
                state = manager.get_status(job_id)
            stdout = manager.job_stdout_contents(job_id).decode()
            return {
                "manager": manager_name,
                "manager_type": manager.manager_type,
                "status": state,
                "return_code": manager.return_code(job_id),
                "stdout": stdout,
                "stderr": manager.job_stderr_contents(job_id).decode(),
                **dict(field.split("=", 1) for field in stdout.split() if "=" in field),
            }
        finally:
            app.shutdown()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("app_yml")
    parser.add_argument("--container")
    parser.add_argument("--command")
    parser.add_argument("--require-slurm", action="store_true")
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args(argv)
    result = run_job(args.app_yml, args.container, args.timeout, command=args.command)
    print(json.dumps(result, indent=2))
    ok = result["status"] == status.COMPLETE and result["return_code"] == 0
    if args.require_slurm and result.get("slurm_job", "none") == "none":
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
