"""The deploy-time check files/check_amqp.py: Pulsar logged in and consumes its setup queue.

Real TLS handshakes against a local server prove that the certificate and the
hostname are verified through Pulsar's own connection code; the broker's answers
are simulated, since issue #4 adds the end-to-end test with Galaxy's RabbitMQ.
"""

import datetime
import importlib.util
import socket
import ssl
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import yaml
from amqp import exceptions as amqp_exceptions
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from tests.helpers import PASSWORD, REPO, render

SPEC = importlib.util.spec_from_file_location("check_amqp", REPO / "files" / "check_amqp.py")
check_amqp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_amqp)


def app_yml(**overrides):
    conf = yaml.safe_load(render("pulsar-app.yml.j2"))
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
        return check_amqp.check(conf or app_yml(), "_default_")


class CheckResultTest(unittest.TestCase):
    def assertResult(self, result, code, *parts):
        self.assertEqual(result[0], code, result[1])
        for part in parts:
            self.assertIn(part, result[1])
        self.assertNotIn(PASSWORD, result[1])

    def test_consumer_on_setup_queue(self):
        channel = FakeChannel(result=1)

        result = run_check(channel)

        self.assertResult(result, check_amqp.OK, "pulsar__setup", "/pulsar/hpc")
        self.assertEqual(channel.declared, [("pulsar__setup", True)])

    def test_queue_without_consumer_is_retried(self):
        self.assertResult(run_check(FakeChannel(result=0)), check_amqp.RETRY, "pulsar__setup")

    def test_missing_queue_is_retried(self):
        error = amqp_exceptions.NotFound(
            "NOT_FOUND - no queue 'pulsar__setup' in vhost '/pulsar/hpc'"
        )
        self.assertResult(
            run_check(FakeChannel(error=error)), check_amqp.RETRY, "journalctl -u pulsar"
        )

    def test_login_refused(self):
        error = amqp_exceptions.AccessRefused(f"ACCESS_REFUSED - Login was refused {PASSWORD}")
        self.assertResult(
            run_check(connect_error=error),
            check_amqp.FAILED,
            "hpc",
            "pulsar_amqp_password",
            "src_galaxy_pulsar_amqp_configs",
        )

    def test_vhost_not_allowed(self):
        error = amqp_exceptions.NotAllowed("NOT_ALLOWED - access to vhost '/pulsar/hpc' refused")
        self.assertResult(
            run_check(connect_error=error), check_amqp.FAILED, "/pulsar/hpc", "pulsar_amqp_name"
        )

    def test_unknown_host(self):
        error = socket.gaierror(-2, "Name or service not known")
        self.assertResult(
            run_check(connect_error=error),
            check_amqp.FAILED,
            "galaxy.example.org",
            "pulsar_galaxy_fqdn",
        )

    def test_unreachable_broker_is_retried(self):
        self.assertResult(
            run_check(connect_error=ConnectionRefusedError(111, "refused")),
            check_amqp.RETRY,
            "galaxy.example.org:5671",
            "src_galaxy_rabbitmq",
        )
        self.assertResult(
            run_check(connect_error=TimeoutError("timed out")), check_amqp.RETRY, "5671"
        )

    def test_unexpected_errors_never_show_the_password(self):
        result = run_check(
            connect_error=RuntimeError(f"pyamqp://hpc:{PASSWORD}@galaxy.example.org")
        )
        self.assertResult(result, check_amqp.FAILED, "RuntimeError")

    def test_only_connection_settings_reach_pulsar(self):
        # As root, Pulsar's acknowledgement stores would create root-owned directories
        # in the pulsar user's persistence directory.
        with tempfile.TemporaryDirectory() as tmp:
            persistence = Path(tmp) / "persisted_data"
            conf = app_yml(persistence_directory=str(persistence))

            exchange = check_amqp.pulsar_exchange(conf, "_default_")

            self.assertFalse(persistence.exists())
            self.assertEqual(exchange.url, conf["message_queue_url"])

    def test_main_reads_app_yml_and_returns_the_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "app.yml"
            path.write_text(render("pulsar-app.yml.j2"))
            with (
                mock.patch.object(
                    check_amqp, "check", return_value=(check_amqp.RETRY, "later")
                ) as check,
                mock.patch("builtins.print") as printed,
            ):
                self.assertEqual(check_amqp.main([str(path), "_default_"]), check_amqp.RETRY)
        check.assert_called_once()
        printed.assert_called_once_with("later")


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


class TlsVerificationTest(unittest.TestCase):
    """Real handshakes through Pulsar's connection code, with our app.yml TLS settings."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.ca_key = ec.generate_private_key(ec.SECP256R1())
        self.ca_cert = certificate("Test CA", self.ca_key, None, self.ca_key, ca=True)
        (self.dir / "ca.pem").write_bytes(pem(self.ca_cert))

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, cert_name, ca_certs=None):
        server = TlsServer(self.dir, cert_name, self.ca_key, self.ca_cert)
        self.addCleanup(server.close)
        # Connect to the local server; the TLS settings stay as rendered, so the
        # hostname checked is still pulsar_galaxy_fqdn (galaxy.example.org).
        url = f"pyamqp://hpc:{PASSWORD}@127.0.0.1:{server.port}//pulsar/hpc"
        conf = app_yml(
            message_queue_url=url, amqp_connect_ssl_ca_certs=ca_certs or str(self.dir / "ca.pem")
        )
        return check_amqp.check(conf, "_default_")

    def test_matching_certificate_passes_tls(self):
        code, message = self.check("galaxy.example.org")

        # TLS succeeded; the local server then closes instead of speaking AMQP.
        self.assertEqual(code, check_amqp.RETRY, message)
        self.assertNotIn("certificate", message)

    def test_certificate_for_another_host_is_rejected(self):
        code, message = self.check("other.example.org")

        self.assertEqual(code, check_amqp.FAILED, message)
        self.assertIn("certificate", message)
        self.assertIn("galaxy.example.org", message)

    def test_untrusted_certificate_is_rejected(self):
        # The system CA bundle, as on the workspace, does not trust the test CA.
        code, message = self.check(
            "galaxy.example.org",
            ca_certs=ssl.get_default_verify_paths().cafile or "/etc/ssl/certs/ca-certificates.crt",
        )

        self.assertEqual(code, check_amqp.FAILED, message)
        self.assertIn("certificate", message)


if __name__ == "__main__":
    unittest.main()
