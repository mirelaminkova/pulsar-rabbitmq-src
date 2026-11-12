"""files/pulsar_main.py: Pulsar exits when a broker consumer thread dies, so systemd restarts it.

pulsar-main's consumer threads quit on an irrecoverable AMQP error (a refused login while
the BYOC account does not exist yet) while the process sleeps on; the launcher turns that
into an exit, and the unit's Restart=always into a retry every RestartSec.
"""

import importlib.util
import runpy
import sys
import threading
import time
from unittest import mock

import pytest
from amqp import exceptions as amqp_exceptions

from tests.helpers import PASSWORD, REPO

SCRIPT = REPO / "files" / "pulsar_main.py"
SPEC = importlib.util.spec_from_file_location("pulsar_main", SCRIPT)
pulsar_main = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pulsar_main)


def fake_pulsar(thread_name, error):
    """Stands in for pulsar.main.main: a consumer thread that dies, then the main loop."""

    def main(argv):
        assert argv == ["--config_dir", "/opt/pulsar/config"]

        def consume():
            raise error

        threading.Thread(name=thread_name, target=consume).start()
        try:
            for _ in range(100):  # pulsar-main's app_loop sleeps until interrupted
                time.sleep(0.05)
        except KeyboardInterrupt:
            return
        raise AssertionError("Pulsar was not interrupted")

    return main


def test_a_dead_consumer_exits_pulsar(capsys):
    error = amqp_exceptions.AccessRefused(f"ACCESS_REFUSED - Login was refused {PASSWORD}")
    with mock.patch.object(pulsar_main.pulsar.main, "main", fake_pulsar("consume-setup-x", error)):
        code = pulsar_main.main(["--config_dir", "/opt/pulsar/config"])

    assert code == 1
    err = capsys.readouterr().err
    assert "consume-setup-x" in err
    assert "AccessRefused" in err
    assert "restart" in err
    assert PASSWORD not in err


def test_pulsar_keeps_running_when_another_thread_dies():
    def main(argv):
        threading.Thread(name="monitor", target=lambda: 1 / 0).start()
        time.sleep(0.2)
        return None

    with mock.patch.object(pulsar_main.pulsar.main, "main", main):
        assert pulsar_main.main([]) == 0


def test_a_real_interrupt_is_not_hidden():
    def main(argv):
        raise KeyboardInterrupt

    with mock.patch.object(pulsar_main.pulsar.main, "main", main), pytest.raises(KeyboardInterrupt):
        pulsar_main.main([])


def test_the_hook_is_restored():
    hook = threading.excepthook
    with mock.patch.object(pulsar_main.pulsar.main, "main", return_value=None):
        pulsar_main.main([])
    assert threading.excepthook is hook


def test_script_runs_as_main(tmp_path, monkeypatch):
    # As the unit runs it: python -I pulsar_main.py PULSAR_MAIN_ARGS...; the pulsar-main
    # arguments reach Pulsar, whose argument parser rejects an unknown one.
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--no-such-option"])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")

    assert exit_info.value.code == 2  # argparse: usage error
