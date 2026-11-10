"""Check that the queues declared in RabbitMQ are the ones Pulsar and Galaxy use.

Needs pulsar-app[amqp] (or pulsar-galaxy-lib and kombu) importable; skipped otherwise.
Galaxy's PulsarMQJobRunner builds the same PulsarExchange from its runner
parameters (amqp_url, manager, amqp_key_prefix), so matching Pulsar's naming
here means Galaxy, Pulsar and the pre-declared queues agree.
"""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from configure import PULSAR_EXCHANGE, PULSAR_QUEUES, pulsar_queue_names  # noqa: E402

try:
    from pulsar.client import amqp_exchange
    from pulsar.client.amqp_exchange_factory import get_exchange
except ImportError:  # pragma: no cover - depends on the test environment
    amqp_exchange = None


@unittest.skipIf(amqp_exchange is None or amqp_exchange.kombu is None, "pulsar-app[amqp] is not installed")
class PulsarQueueNamesTest(unittest.TestCase):
    def pulsar_names(self, manager, params):
        exchange = get_exchange("amqp://user:pw@127.0.0.1:5672/pulsar", manager, params)
        return [exchange._PulsarExchange__queue_name(name) for name in PULSAR_QUEUES]

    def test_exchange(self):
        self.assertEqual(amqp_exchange.DEFAULT_EXCHANGE_NAME, PULSAR_EXCHANGE)
        self.assertEqual(amqp_exchange.DEFAULT_EXCHANGE_TYPE, "direct")

    def test_default_manager(self):
        self.assertEqual(self.pulsar_names("_default_", {}), pulsar_queue_names())

    def test_named_manager(self):
        self.assertEqual(self.pulsar_names("gpu", {}), pulsar_queue_names(manager="gpu"))

    def test_key_prefix(self):
        self.assertEqual(self.pulsar_names("_default_", {"amqp_key_prefix": "src"}),
                         pulsar_queue_names(amqp_key_prefix="src"))

    def test_ack_suffix(self):
        self.assertEqual(amqp_exchange.ACK_QUEUE_SUFFIX, "_ack")


if __name__ == "__main__":
    unittest.main()
