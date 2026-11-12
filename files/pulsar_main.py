"""Run Pulsar (pulsar-main) and exit when one of its broker consumers dies.

Usage (the pulsar unit, with Pulsar's venv Python): pulsar_main.py PULSAR_MAIN_ARGS...

Pulsar consumes its queues on the Galaxy's RabbitMQ in threads. On an irrecoverable
AMQP error, such as the refused login while the BYOC account does not exist yet
(byoc-sync creates it once its pull request is merged), a consumer thread quits while
pulsar-main sleeps on without consumers, so Pulsar would never connect by itself once
the account exists. This launcher exits with status 1 instead, and the unit's
Restart=always starts Pulsar again every RestartSec until it connects.
Prints the thread's name (Pulsar's: the queue and the URL with the password masked) and
the error's type, never the error's text.
"""

import _thread
import sys
import threading

import pulsar.main

CONSUMER_PREFIX = "consume-"  # pulsar.messaging.bind_amqp's thread names
# The consumer's own heartbeat thread (pulsar.client.amqp_exchange): its errors are the
# connection's, which the consumer recovers from by reconnecting.
HEARTBEAT_PREFIX = "consume-heartbeat-"
EXIT_CONSUMER_DIED = 1

dead = []


def consumer_died(args):
    """threading.excepthook: interrupt pulsar-main's loop when a consumer thread dies."""
    name = getattr(args.thread, "name", "")
    if not name.startswith(CONSUMER_PREFIX) or name.startswith(HEARTBEAT_PREFIX):
        threading.__excepthook__(args)  # as without this launcher
        return
    # Pulsar has logged the exception itself ("consumer quitting in problematic fashion").
    dead.append((name, args.exc_type.__name__))
    _thread.interrupt_main()  # KeyboardInterrupt: pulsar-main shuts down cleanly


def main(argv):
    """Return the exit status: 0 as pulsar-main returned, 1 when a consumer died."""
    dead.clear()
    previous = threading.excepthook
    threading.excepthook = consumer_died
    try:
        pulsar.main.main(argv)
    except KeyboardInterrupt:
        if not dead:
            raise
    finally:
        threading.excepthook = previous
    if dead:
        name, error = dead[0]
        print(
            f"Pulsar's broker consumer {name} quit ({error}): exiting so that systemd can "
            "restart Pulsar; it reconnects once the BYOC account exists.",
            file=sys.stderr,
        )
        return EXIT_CONSUMER_DIED
    return 0


def run():
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
