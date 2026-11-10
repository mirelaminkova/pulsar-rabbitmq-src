"""Check credential retention, generated RabbitMQ files and input handling without deployment privileges."""
import base64
import hashlib
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from configure import configure, pulsar_queue_names, rabbitmq_password_hash  # noqa: E402


def load(directory, name):
    return json.loads((Path(directory) / ".secrets" / name).read_text()) if name.endswith(".json") \
        else (Path(directory) / ".secrets" / name).read_text()


def verify_hash(encoded, password):
    raw = base64.b64decode(encoded)
    salt, digest = raw[:4], raw[4:]
    return hashlib.sha256(salt + password.encode()).digest() == digest


class ConfigurationTest(unittest.TestCase):
    def test_rerun_preserves_credentials_and_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            self.assertTrue(configure(directory))
            original = (path / "secrets.json").read_bytes()
            definitions = (path / ".secrets" / "definitions.json").read_bytes()
            # Identical inputs give identical files, so reruns do not restart RabbitMQ.
            self.assertFalse(configure(directory))
            self.assertEqual(original, (path / "secrets.json").read_bytes())
            self.assertEqual(definitions, (path / ".secrets" / "definitions.json").read_bytes())
            for name in ["secrets.json", ".env"]:
                self.assertEqual(stat.S_IMODE((path / name).stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((path / ".secrets").stat().st_mode), 0o700)
            for name in ["rabbitmq.conf", "definitions.json"]:
                self.assertEqual(stat.S_IMODE((path / ".secrets" / name).stat().st_mode), 0o444)
            values = json.loads(original)
            self.assertEqual(set(values), {"RABBITMQ_PAIR_PASSWORD", "RABBITMQ_ADMIN_PASSWORD"})
            self.assertNotEqual(values["RABBITMQ_PAIR_PASSWORD"], values["RABBITMQ_ADMIN_PASSWORD"])

    def test_definitions_users_vhost_and_password_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            configure(directory, vhost="jobs")
            values = json.loads((Path(directory) / "secrets.json").read_text())
            definitions = load(directory, "definitions.json")
            users = {user["name"]: user for user in definitions["users"]}
            self.assertEqual(set(users), {"galaxy-pulsar", "admin"})
            self.assertEqual(users["admin"]["tags"], ["administrator"])
            self.assertEqual(users["galaxy-pulsar"]["tags"], [])
            self.assertTrue(verify_hash(users["galaxy-pulsar"]["password_hash"], values["RABBITMQ_PAIR_PASSWORD"]))
            self.assertTrue(verify_hash(users["admin"]["password_hash"], values["RABBITMQ_ADMIN_PASSWORD"]))
            self.assertFalse(verify_hash(users["admin"]["password_hash"], values["RABBITMQ_PAIR_PASSWORD"]))
            self.assertEqual(definitions["vhosts"], [{"name": "jobs"}])
            self.assertEqual({p["user"] for p in definitions["permissions"] if p["vhost"] == "jobs"},
                             {"galaxy-pulsar", "admin"})
            # No plaintext password anywhere in the files the container sees.
            for name in ["definitions.json", "rabbitmq.conf"]:
                text = (Path(directory) / ".secrets" / name).read_text()
                for secret in values.values():
                    self.assertNotIn(secret, text)

    def test_pulsar_exchange_and_queues_are_declared(self):
        with tempfile.TemporaryDirectory() as directory:
            configure(directory)
            definitions = load(directory, "definitions.json")
            self.assertEqual(definitions["exchanges"][0]["name"], "pulsar")
            self.assertEqual(definitions["exchanges"][0]["type"], "direct")
            queues = {queue["name"] for queue in definitions["queues"]}
            self.assertEqual(queues, {"pulsar__setup", "pulsar__kill", "pulsar__status", "pulsar__status_update"})
            self.assertEqual({b["destination"] for b in definitions["bindings"]}, queues)
            self.assertTrue(all(b["routing_key"] == b["destination"] for b in definitions["bindings"]))
            self.assertTrue(all(q["durable"] and not q["auto_delete"] and q["arguments"] == {}
                                for q in definitions["queues"]))

    def test_queue_names_follow_pulsar(self):
        self.assertIn("pulsar__setup", pulsar_queue_names())
        self.assertIn("pulsar_gpu__setup", pulsar_queue_names(manager="gpu"))
        self.assertIn("myprefix_status_update", pulsar_queue_names(amqp_key_prefix="myprefix"))
        self.assertIn("pulsar__status_update_ack", pulsar_queue_names(acknowledge=True))
        self.assertNotIn("pulsar__status_update_ack", pulsar_queue_names())

    def test_wizard_pair_password_replaces_generated_one_and_is_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            configure(directory)
            generated = json.loads((path / "secrets.json").read_text())
            chosen = "Chosen-pair.pass_2026~ok"
            self.assertTrue(configure(directory, pair_password=chosen))
            values = json.loads((path / "secrets.json").read_text())
            self.assertEqual(values["RABBITMQ_PAIR_PASSWORD"], chosen)
            self.assertEqual(values["RABBITMQ_ADMIN_PASSWORD"], generated["RABBITMQ_ADMIN_PASSWORD"])
            # Leaving the parameter empty later keeps the chosen password.
            self.assertFalse(configure(directory))
            self.assertEqual(json.loads((path / "secrets.json").read_text())["RABBITMQ_PAIR_PASSWORD"], chosen)

    def test_broker_config(self):
        with tempfile.TemporaryDirectory() as directory:
            configure(directory, consumer_timeout_ms=7200000)
            config = load(directory, "rabbitmq.conf")
            self.assertIn("load_definitions = /etc/rabbitmq/definitions.json", config)
            self.assertIn("consumer_timeout = 7200000", config)
            self.assertIn("management.path_prefix = /rabbitmq", config)
            configure(directory, management_path_prefix="")
            self.assertNotIn("path_prefix", load(directory, "rabbitmq.conf"))
            env = (Path(directory) / ".env").read_text()
            self.assertIn("RABBITMQ_AMQP_PORT=5672", env)

    def test_rejects_bad_input(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = [
                dict(pair_password="short"),
                dict(pair_password="has,comma-and-long-enough"),
                dict(pair_password="has:colon-and-long-enough"),
                dict(pair_password="has@at-sign-long-enough"),
                dict(pair_username="x"),
                dict(pair_username="admin"),
                dict(vhost="/"),
                dict(amqp_port=80),
                dict(consumer_timeout_ms=10),
                dict(manager="bad name"),
                dict(amqp_key_prefix="bad/prefix"),
                dict(management_path_prefix="rabbitmq"),
            ]
            for kwargs in bad:
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    configure(directory, **kwargs)

    def test_rejects_tampered_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            configure(directory)
            path = Path(directory) / "secrets.json"
            values = json.loads(path.read_text())
            values["EXTRA"] = "x"
            path.write_text(json.dumps(values))
            with self.assertRaises(ValueError):
                configure(directory)

    def test_hash_is_deterministic_and_salted_per_user(self):
        self.assertEqual(rabbitmq_password_hash("a-user", "p" * 20), rabbitmq_password_hash("a-user", "p" * 20))
        self.assertNotEqual(rabbitmq_password_hash("a-user", "p" * 20), rabbitmq_password_hash("b-user", "p" * 20))


if __name__ == "__main__":
    unittest.main()
