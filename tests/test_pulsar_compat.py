"""Our app.yml as Pulsar 0.15.15 reads it (needs pulsar-app[amqp], tests/requirements.txt).

Pulsar builds its kombu connection with pulsar.messaging.bind_amqp.get_exchange from
app.yml; these tests go through the same function.
"""

import ssl
import unittest

from pulsar.messaging import bind_amqp

from tests.helpers import PASSWORD, render_yaml, resolve


def connection(app_yml, manager="_default_"):
    # Without persistence_directory: the acknowledgement stores would create it.
    params = {key: value for key, value in app_yml.items() if key != "persistence_directory"}
    exchange = bind_amqp.get_exchange(app_yml["message_queue_url"], manager, params)
    return exchange, exchange.connection(exchange.url)


class ConnectionTest(unittest.TestCase):
    def test_pulsar_connects_as_the_galaxy_user_to_its_vhost(self):
        _, conn = connection(render_yaml("pulsar-app.yml.j2"))

        self.assertEqual(conn.transport_cls, "pyamqp")
        self.assertEqual(conn.hostname, "galaxy.example.org")
        self.assertEqual(conn.port, 5671)
        self.assertEqual(conn.userid, "hpc")
        self.assertEqual(conn.password, PASSWORD)
        self.assertEqual(conn.virtual_host, "/pulsar/hpc")

    def test_tls_verifies_certificate_and_hostname(self):
        _, conn = connection(render_yaml("pulsar-app.yml.j2"))

        self.assertEqual(
            conn.ssl,
            {
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": "/etc/ssl/certs/ca-certificates.crt",
                # py-amqp only checks the hostname when server_hostname is set.
                "server_hostname": "galaxy.example.org",
            },
        )

    def test_ssl_query_would_discard_the_tls_settings(self):
        # Why message_queue_url has no ?ssl=1: kombu lets the URL's ssl replace the
        # ssl dict Pulsar builds from amqp_connect_ssl_*, so nothing would be verified.
        app_yml = render_yaml("pulsar-app.yml.j2")
        app_yml["message_queue_url"] += "?ssl=1"

        _, conn = connection(app_yml)

        self.assertNotIsInstance(conn.ssl, dict)

    def test_acknowledgement_and_publish_retries(self):
        exchange, _ = connection(render_yaml("pulsar-app.yml.j2"))

        self.assertEqual(exchange._PulsarExchange__republish_time, 1200)
        self.assertTrue(exchange._PulsarExchange__publish_kwds["retry"])
        self.assertEqual(exchange._PulsarExchange__publish_kwds["retry_policy"]["max_retries"], 60)


class QueueNamesTest(unittest.TestCase):
    def test_setup_queue(self):
        for manager in ("_default_", "gpu"):
            with self.subTest(manager=manager):
                exchange, _ = connection(
                    render_yaml("pulsar-app.yml.j2", pulsar_manager=manager), manager
                )

                self.assertEqual(
                    resolve("_pulsar_amqp", pulsar_manager=manager)["setup_queue"],
                    exchange._PulsarExchange__queue_name("setup"),
                )


if __name__ == "__main__":
    unittest.main()
