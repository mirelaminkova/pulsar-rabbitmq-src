"""The deploy-time check files/check_amqp.py: Pulsar logged in and consumes its setup queue.

Real TLS handshakes against a local server prove that the certificate and the
hostname are verified through Pulsar's own connection code; the broker's answers
are simulated; this repository's issue #4 adds the end-to-end test with Galaxy's RabbitMQ.
"""

import datetime
import importlib.util
import runpy
import socket
import ssl
import subprocess
import sys
import threading
from unittest import mock

import pytest
import yaml
from amqp import exceptions as amqp_exceptions
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from tests.helpers import MQ_USER, PASSWORD, REPO, VHOST, render

SCRIPT = REPO / "files" / "check_amqp.py"
SPEC = importlib.util.spec_from_file_location("check_amqp", SCRIPT)
check_amqp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_amqp)


def app_yml(params=None, **overrides):
    """Our rendered app.yml, from component ``params``, with ``overrides`` applied."""
    conf = yaml.safe_load(render("pulsar-app.yml.j2", **(params or {})))
    conf.update(overrides)
    return conf


class FakeChannel:
    def __init__(self, result=None, error=None):
        self.result, self.error, self.declared = result, error, []

    def queue_declare(self, queue, passive):
        self.declared.append((queue, passive))
        if self.error:
            raise self.error
        return (queue, 0, self.result)


def run_check(channel=None, connect_error=None, conf=None):
    connection = mock.MagicMock()
    connection.__enter__.return_value = connection
    if connect_error:
        connection.connect.side_effect = connect_error
    connection.channel.return_value = channel or FakeChannel(result=1)
    with mock.patch.object(check_amqp, "open_connection", return_value=connection):
        return check_amqp.check(conf or app_yml())


def assert_result(result, code, *parts):
    assert result[0] == code, result[1]
    for part in parts:
        assert part in result[1]
    assert PASSWORD not in result[1]


def test_consumer_on_setup_queue():
    channel = FakeChannel(result=1)

    result = run_check(channel)

    assert_result(result, check_amqp.OK, "pulsar_production__setup", VHOST)
    assert channel.declared == [("pulsar_production__setup", True)]


def test_manager_comes_from_app_yml():
    channel = FakeChannel(result=1)
    conf = app_yml({"pulsar_manager": "gpu"})

    assert_result(run_check(channel, conf=conf), check_amqp.OK, "pulsar_gpu__setup")


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param([str(SCRIPT), "/nonexistent/app.yml"], id="script-raises"),
        pytest.param(
            [str(REPO / "files" / "no_such_check.py"), "/nonexistent/app.yml"], id="no-script"
        ),
    ],
)
def test_a_broken_run_is_not_retried(argv):
    # Python exits 2 when it cannot run the script and 1 on an exception; only the
    # check's own "not yet" status may be retried.
    result = subprocess.run([sys.executable, *argv], capture_output=True, check=False)  # noqa: S603 - fixed argv
    assert result.returncode not in (check_amqp.OK, check_amqp.RETRY)


def test_queue_without_consumer_is_retried():
    assert_result(run_check(FakeChannel(result=0)), check_amqp.RETRY, "pulsar_production__setup")


def test_missing_queue_is_retried():
    error = amqp_exceptions.NotFound(
        f"NOT_FOUND - no queue 'pulsar_production__setup' in vhost '{VHOST}'"
    )
    assert_result(run_check(FakeChannel(error=error)), check_amqp.RETRY, "journalctl -u pulsar")


def test_exit_statuses_are_the_playbook_contract():
    # 0 connected, 75 not yet (retried), 3 waiting for the BYOC account, 1 an error: the
    # playbook maps them to the states of byoc-state.json and fails only for an error.
    assert (check_amqp.OK, check_amqp.RETRY, check_amqp.WAITING, check_amqp.FAILED) == (0, 75, 3, 1)


def test_login_refused_is_waiting_for_the_account():
    # Normal at deployment: byoc-sync creates the user only once its pull request is merged.
    error = amqp_exceptions.AccessRefused(f"ACCESS_REFUSED - Login was refused {PASSWORD}")
    assert_result(
        run_check(connect_error=error),
        check_amqp.WAITING,
        MQ_USER,
        "pulsar_amqp_name",
        "pulsar_amqp_password",
        "byoc_pulsar",
        "merged",
    )


def test_vhost_not_allowed_is_waiting_for_the_vhost():
    error = amqp_exceptions.NotAllowed(f"NOT_ALLOWED - access to vhost '{VHOST}' refused")
    assert_result(run_check(connect_error=error), check_amqp.WAITING, VHOST, MQ_USER, "byoc_pulsar")


def test_waiting_exits_3_from_the_script(tmp_path, monkeypatch, capsys):
    # As the playbook runs it: 3 is "waiting", recorded and not retried, not a failure.
    path = tmp_path / "app.yml"
    path.write_text(render("pulsar-app.yml.j2"))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(path)])
    error = amqp_exceptions.AccessRefused("ACCESS_REFUSED - Login was refused")
    connection = mock.MagicMock()
    connection.__enter__.return_value = connection
    connection.connect.side_effect = error

    exchange = mock.MagicMock()
    exchange.connection.return_value = connection
    exchange._PulsarExchange__queue_name.return_value = "q"

    patched = mock.patch.object(check_amqp.bind_amqp, "get_exchange", return_value=exchange)
    with patched, pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")

    assert exit_info.value.code == 3
    assert "refused the login" in capsys.readouterr().out


def test_unknown_host():
    error = socket.gaierror(-2, "Name or service not known")
    assert_result(
        run_check(connect_error=error),
        check_amqp.FAILED,
        "mq.example.org",
        "pulsar_amqp_host",
    )


EU = {"pulsar_amqp_host": "mq.galaxyproject.eu"}


def test_messages_show_the_broker_host():
    result = run_check(FakeChannel(result=1), conf=app_yml(EU))

    assert_result(result, check_amqp.OK, "mq.galaxyproject.eu:5671", MQ_USER)


def test_temporary_dns_failure_is_retried():
    error = socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
    assert_result(run_check(connect_error=error), check_amqp.RETRY, "mq.example.org")


@pytest.mark.parametrize(
    "error",
    [ConnectionRefusedError(111, "refused"), TimeoutError("timed out")],
    ids=lambda error: type(error).__name__,
)
def test_unreachable_broker_is_retried(error):
    assert_result(
        run_check(connect_error=error),
        check_amqp.RETRY,
        "mq.example.org:5671",
        "allow this workspace",
    )


def test_unexpected_errors_never_show_the_password():
    result = run_check(connect_error=RuntimeError(f"pyamqp://{MQ_USER}:{PASSWORD}@mq.example.org"))
    assert_result(result, check_amqp.FAILED, "RuntimeError")


def test_only_connection_settings_reach_pulsar(tmp_path):
    # As root, Pulsar's acknowledgement stores would create root-owned directories
    # in the pulsar user's persistence directory.
    persistence = tmp_path / "persisted_data"
    conf = app_yml(persistence_directory=str(persistence))

    exchange = check_amqp.pulsar_exchange(conf)

    assert not persistence.exists()
    assert exchange.url == conf["message_queue_url"]


def test_main_reads_app_yml_and_returns_the_code(tmp_path, capsys):
    path = tmp_path / "app.yml"
    path.write_text(render("pulsar-app.yml.j2"))
    with mock.patch.object(check_amqp, "check", return_value=(check_amqp.RETRY, "later")) as check:
        assert check_amqp.main([str(path)]) == check_amqp.RETRY
    check.assert_called_once()
    assert capsys.readouterr().out == "later\n"


def closed_port():
    """A local port nothing listens on."""
    with socket.create_server(("127.0.0.1", 0)) as sock:
        return sock.getsockname()[1]


def test_script_exits_with_the_check_status(tmp_path, monkeypatch, capsys):
    # As the playbook runs it: `python check_amqp.py app.yml`, retried while it exits 75.
    path = tmp_path / "app.yml"
    url = f"pyamqp://{MQ_USER}:{PASSWORD}@127.0.0.1:{closed_port()}/{VHOST}"
    path.write_text(yaml.safe_dump(app_yml(message_queue_url=url)))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), str(path)])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(SCRIPT), run_name="__main__")

    assert exit_info.value.code == check_amqp.RETRY
    output = capsys.readouterr().out
    assert "Cannot reach RabbitMQ on 127.0.0.1" in output
    assert PASSWORD not in output


def certificate(name, issuer_key, issuer_name, key, ca=False):
    now = datetime.datetime.now(datetime.timezone.utc)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_name or subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if not ca:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(name)]), critical=False
        )
    return builder.sign(issuer_key, hashes.SHA256())


def pem(cert):
    return cert.public_bytes(serialization.Encoding.PEM)


class TlsServer:
    """Accepts TLS connections for ``name``, then closes them (no AMQP)."""

    def __init__(self, directory, name, ca_key, ca_cert):
        key = ec.generate_private_key(ec.SECP256R1())
        cert = certificate(name, ca_key, ca_cert.subject, key)
        (directory / "server.pem").write_bytes(
            pem(cert)
            + key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(directory / "server.pem")
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            try:
                with self.context.wrap_socket(client, server_side=True) as tls:
                    tls.recv(8)  # AMQP protocol header
            except (ssl.SSLError, OSError):
                pass

    def close(self):
        self.sock.close()


class PlainAmqpServer:
    """Answers like RabbitMQ's plain AMQP port (5672): the protocol header, no TLS."""

    def __init__(self):
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            with client:
                client.recv(1024)  # the TLS ClientHello
                client.sendall(b"AMQP\x00\x00\x09\x01")

    def close(self):
        self.sock.close()


@pytest.fixture
def ca(tmp_path):
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_cert = certificate("Test CA", ca_key, None, ca_key, ca=True)
    (tmp_path / "ca.pem").write_bytes(pem(ca_cert))
    return ca_key, ca_cert


@pytest.fixture
def tls_check(tmp_path, ca):
    """Real handshakes through Pulsar's connection code, with our app.yml TLS settings."""
    servers = []

    def check(cert_name, ca_certs=None, params=None):
        server = TlsServer(tmp_path, cert_name, *ca)
        servers.append(server)
        # Connect to the local server; the TLS settings stay as rendered, so the
        # hostname checked is still the broker's (pulsar_amqp_host, mq.example.org,
        # unless params say otherwise).
        url = f"pyamqp://{MQ_USER}:{PASSWORD}@127.0.0.1:{server.port}/{VHOST}"
        conf = app_yml(
            params,
            message_queue_url=url,
            amqp_connect_ssl_ca_certs=ca_certs or str(tmp_path / "ca.pem"),
        )
        return check_amqp.check(conf)

    yield check
    for server in servers:
        server.close()


def test_matching_certificate_passes_tls(tls_check):
    code, message = tls_check("mq.example.org")

    # TLS succeeded; the local server then closes instead of speaking AMQP.
    assert code == check_amqp.RETRY, message
    assert "certificate" not in message


def test_certificate_for_another_host_is_rejected(tls_check):
    code, message = tls_check("other.example.org")

    assert code == check_amqp.FAILED, message
    assert "certificate" in message
    assert "mq.example.org" in message
    assert "pulsar_amqp_host" in message


def test_certificate_for_the_broker_host_passes_tls(tls_check):
    code, message = tls_check("mq.galaxyproject.eu", params=EU)

    assert code == check_amqp.RETRY, message
    assert "certificate" not in message


def test_certificate_for_the_galaxy_host_is_rejected(tls_check):
    # The Galaxy server's own name is not the broker's: only pulsar_amqp_host is verified.
    code, message = tls_check("usegalaxy.eu", params=EU)

    assert code == check_amqp.FAILED, message
    assert "certificate" in message
    assert "mq.galaxyproject.eu" in message
    assert "pulsar_amqp_host" in message


def test_untrusted_certificate_is_rejected(tls_check):
    # The system CA bundle, as on the workspace, does not trust the test CA.
    code, message = tls_check(
        "mq.example.org",
        ca_certs=ssl.get_default_verify_paths().cafile or "/etc/ssl/certs/ca-certificates.crt",
    )

    assert code == check_amqp.FAILED, message
    assert "certificate" in message


def test_plain_amqp_port_is_a_tls_failure():
    # pulsar_amqp_port set to RabbitMQ's plain port: the server does not speak TLS.
    server = PlainAmqpServer()
    try:
        url = f"pyamqp://{MQ_USER}:{PASSWORD}@127.0.0.1:{server.port}/{VHOST}"
        code, message = check_amqp.check(app_yml(message_queue_url=url))
    finally:
        server.close()

    assert code == check_amqp.FAILED, message
    assert "TLS with 127.0.0.1" in message
    assert "pulsar_amqp_port" in message
    assert "certificate" not in message
