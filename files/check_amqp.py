"""Check that Pulsar logged in to the Galaxy's RabbitMQ broker and consumes its setup queue.

The broker is pulsar_amqp_host (usegalaxy.eu's mq.galaxyproject.eu), the login this
Pulsar's BYOC user galaxy_<name>_byoc, created by the byoc-sync pipeline from the
byoc_pulsar form of the user's Galaxy preferences.

Usage (with Pulsar's venv Python): check_amqp.py APP_YML

Connects with the URL and TLS settings of Pulsar's app.yml through Pulsar's own
connection code, and passively declares the queue Galaxy sends new jobs to. A
consumer on it means Pulsar is logged in and waiting for jobs; the Galaxy broker
has no management API that Pulsar's user could ask instead.

Exit status: 0 consuming, 1 failed (fix the configuration), 75 not yet (retry;
EX_TEMPFAIL, unlike 2, which Python itself returns when it cannot run a script).
Prints one line and never the password.

A consumer may also be a stale connection of an earlier Pulsar with the same name
and password (until its heartbeat expires); give each Pulsar its own name.
"""

import socket
import ssl
import sys
from urllib.parse import urlsplit

import yaml
from amqp import exceptions as amqp_exceptions
from pulsar.messaging import bind_amqp

OK, FAILED, RETRY = 0, 1, 75
# Only the settings needed to connect: with persistence_directory and amqp_acknowledge
# Pulsar's exchange would create acknowledgement stores, here as root.
CONNECT_PREFIXES = ("amqp_connect_ssl_", "amqp_key_prefix")
# Where the user enters the BYOC name and password that byoc-sync acts on.
FORM = "the byoc_pulsar form of the Galaxy preferences"


def manager_name(conf):
    """The single manager of app.yml, the one Galaxy's runner uses."""
    [name] = conf["managers"]
    return name


def pulsar_exchange(conf):
    params = {key: value for key, value in conf.items() if key.startswith(CONNECT_PREFIXES)}
    return bind_amqp.get_exchange(conf["message_queue_url"], manager_name(conf), params)


def open_connection(exchange):
    return exchange.connection(exchange.url)


def check(conf):
    """Return (exit status, message)."""
    url = urlsplit(conf["message_queue_url"])
    exchange = pulsar_exchange(conf)
    queue = exchange._PulsarExchange__queue_name("setup")  # the name Pulsar consumes
    where = {
        "address": f"{url.hostname}:{url.port}",
        "host": url.hostname,
        # The name the certificate is checked for: pulsar_amqp_host.
        "server_hostname": conf.get("amqp_connect_ssl_server_hostname") or url.hostname,
        "user": url.username,
        "vhost": url.path[1:],
        "queue": queue,
    }
    try:
        with open_connection(exchange) as connection:
            connection.connect()
            _, _, consumers = connection.channel().queue_declare(queue=queue, passive=True)
    except Exception as error:
        code, message = explain(error, where)
        return code, message.replace(url.password or "\0", "***")
    if consumers < 1:
        return RETRY, (
            "Queue {queue} exists in vhost {vhost} on {address}, but no Pulsar consumes it: "
            "check `journalctl -u pulsar`.".format(**where)
        )
    return OK, "Pulsar consumes {queue} in vhost {vhost} on {address} as {user}.".format(**where)


def explain(error, where):
    # Most specific first: certificate errors are SSLErrors, which are OSErrors.
    if isinstance(error, ssl.SSLCertVerificationError):
        return FAILED, (
            "The TLS certificate of {address} was rejected ({reason}). The certificate must be "
            "for the broker's hostname {server_hostname} (pulsar_amqp_host)."
        ).format(reason=error.verify_message or error.reason, **where)
    if isinstance(error, ssl.SSLError):
        return (
            FAILED,
            f"TLS with {where['address']} failed ({error.reason}): "
            "is pulsar_amqp_port the broker's AMQPS port?",
        )
    if isinstance(error, socket.gaierror) and error.errno == socket.EAI_AGAIN:
        return RETRY, f"Resolving {where['host']} failed temporarily ({error.strerror})."
    if isinstance(error, socket.gaierror):
        return FAILED, f"Cannot resolve {where['host']}: check pulsar_amqp_host."
    if isinstance(error, amqp_exceptions.AccessRefused):
        return FAILED, (
            "RabbitMQ on {address} refused the login of user {user}: this Pulsar's name and "
            "password (pulsar_amqp_name and pulsar_amqp_password, or the generated ones on the "
            "welcome page) must be the ones entered in " + FORM + ", and byoc-sync's pull "
            "request for them must have been merged and deployed."
        ).format(**where)
    if isinstance(error, amqp_exceptions.NotAllowed):
        return FAILED, (
            "User {user} may not use vhost {vhost} on {address}: byoc-sync creates both from "
            "the username in " + FORM + "; check pulsar_amqp_name against it."
        ).format(**where)
    if isinstance(error, amqp_exceptions.NotFound):
        return RETRY, (
            "Queue {queue} does not exist yet in vhost {vhost}: Pulsar has not connected; "
            "check `journalctl -u pulsar`."
        ).format(**where)
    if isinstance(error, (OSError, amqp_exceptions.ConnectionError)):
        return RETRY, (
            "Cannot reach RabbitMQ on {address} ({kind}): the broker must allow this workspace "
            "on that port (its firewall or security group)."
        ).format(kind=type(error).__name__, **where)
    return (
        FAILED,
        f"Checking RabbitMQ on {where['address']} failed: {type(error).__name__}: {error}",
    )


def main(argv):
    [app_yml] = argv
    with open(app_yml) as stream:
        conf = yaml.safe_load(stream)
    code, message = check(conf)
    print(message)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
