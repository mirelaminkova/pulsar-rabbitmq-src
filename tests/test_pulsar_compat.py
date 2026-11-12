"""Our app.yml as Pulsar 0.15.15 reads it (needs pulsar-app[amqp], requirements-dev.txt).

Pulsar builds its kombu connection with pulsar.messaging.bind_amqp.get_exchange from
app.yml; these tests go through the same function.
"""

import ssl

import pytest
from pulsar.messaging import bind_amqp

from tests.helpers import PASSWORD, render, render_yaml, resolve


def connection(app_yml, manager="_default_"):
    # Without persistence_directory: the acknowledgement stores would create it.
    params = {key: value for key, value in app_yml.items() if key != "persistence_directory"}
    exchange = bind_amqp.get_exchange(app_yml["message_queue_url"], manager, params)
    return exchange, exchange.connection(exchange.url)


def test_pulsar_connects_as_the_galaxy_user_to_its_vhost():
    _, conn = connection(render_yaml("pulsar-app.yml.j2"))

    assert conn.transport_cls == "pyamqp"
    assert conn.hostname == "galaxy.example.org"
    assert conn.port == 5671
    assert conn.userid == "hpc"
    assert conn.password == PASSWORD
    assert conn.virtual_host == "/pulsar/hpc"


def test_tls_verifies_certificate_and_hostname():
    _, conn = connection(render_yaml("pulsar-app.yml.j2"))

    assert conn.ssl == {
        "cert_reqs": ssl.CERT_REQUIRED,
        "ca_certs": "/etc/ssl/certs/ca-certificates.crt",
        # py-amqp only checks the hostname when server_hostname is set.
        "server_hostname": "galaxy.example.org",
    }


def test_a_separate_broker_host_is_connected_to_and_verified():
    # usegalaxy.eu's Pulsar Network: the broker is not the Galaxy host.
    app_yml = render_yaml(
        "pulsar-app.yml.j2",
        pulsar_galaxy_fqdn="usegalaxy.eu",
        pulsar_amqp_host="mq.galaxyproject.eu",
    )

    _, conn = connection(app_yml)

    assert conn.hostname == "mq.galaxyproject.eu"
    assert conn.ssl["server_hostname"] == "mq.galaxyproject.eu"
    assert conn.ssl["cert_reqs"] == ssl.CERT_REQUIRED
    assert "usegalaxy.eu" not in app_yml["message_queue_url"]


def test_app_yml_is_the_same_with_and_without_an_empty_broker_host():
    # Existing catalog items (no pulsar_amqp_host) get the app.yml they had.
    assert render("pulsar-app.yml.j2", pulsar_amqp_host="") == render("pulsar-app.yml.j2")
    assert render("pulsar-app.yml.j2", pulsar_amqp_host="galaxy.example.org") == render(
        "pulsar-app.yml.j2"
    )


def test_ssl_query_would_discard_the_tls_settings():
    # Why message_queue_url has no ?ssl=1: kombu lets the URL's ssl replace the
    # ssl dict Pulsar builds from amqp_connect_ssl_*, so nothing would be verified.
    app_yml = render_yaml("pulsar-app.yml.j2")
    app_yml["message_queue_url"] += "?ssl=1"

    _, conn = connection(app_yml)

    assert not isinstance(conn.ssl, dict)


def test_acknowledgement_and_publish_retries():
    exchange, _ = connection(render_yaml("pulsar-app.yml.j2"))

    assert exchange._PulsarExchange__republish_time == 1200
    assert exchange._PulsarExchange__publish_kwds["retry"]
    assert exchange._PulsarExchange__publish_kwds["retry_policy"]["max_retries"] == 60


def test_republish_time_follows_the_parameter():
    # usegalaxy.eu's runner republishes after 300 s, the Galaxy component's after 1200 s.
    app_yml = render_yaml("pulsar-app.yml.j2", pulsar_amqp_ack_republish_time="300")
    exchange, _ = connection(app_yml)

    assert app_yml["amqp_ack_republish_time"] == 300
    assert exchange._PulsarExchange__republish_time == 300


@pytest.mark.parametrize("manager", ["_default_", "gpu"])
def test_setup_queue(manager):
    exchange, _ = connection(render_yaml("pulsar-app.yml.j2", pulsar_manager=manager), manager)

    assert resolve("_pulsar_amqp", pulsar_manager=manager)["setup_queue"] == (
        exchange._PulsarExchange__queue_name("setup")
    )
