# Vendored Galaxy contract

Verbatim copies from [mirelaminkova/src-component-galaxy-amqp](https://github.com/mirelaminkova/src-component-galaxy-amqp)
at commit `5bd3bf72bb31db96f7a5af99c21abc9c9bf3473a`:

| File here | File in the Galaxy repository |
| --- | --- |
| `src_galaxy.py` | `filter_plugins/src_galaxy.py` (parser and validation of `src_galaxy_pulsar_amqp_configs`) |
| `pulsar_amqp_app.yml.j2` | `templates/rabbitmq/pulsar_amqp_app.yml.j2` (the `app.yml` settings Galaxy expects from this Pulsar) |
| `job_conf.yml.j2` | `templates/galaxy/config/job_conf.yml.j2` (Galaxy's `PulsarMQJobRunner` runner for this Pulsar) |
| `destinations.yml.j2` | `files/galaxy/tpv/destinations.yml.j2` (Galaxy's TPV destination for this Pulsar) |
| `rabbitmq.conf.j2` | `templates/rabbitmq/rabbitmq.conf.j2` (Galaxy's RabbitMQ listeners and TLS settings) |

The tests feed this component's output through these files, so the welcome page's
`src_galaxy_pulsar_amqp_configs` entry and Pulsar's `app.yml` are checked against the
Galaxy side's own code; the end-to-end test (`tests/e2e/`) takes Galaxy's runner and
destination parameters for this Pulsar from `job_conf.yml.j2` and `destinations.yml.j2`,
and molecule's test broker (`molecule/default/prepare.yml`) is configured with `rabbitmq.conf.j2`.
Do not edit them; copy them again when the Galaxy repository changes. `GALAXY_AMQP_REPO=/path/to/checkout pytest tests/test_galaxy_contract.py --no-cov`
compares them with a checkout (CI does this against the Galaxy repository's `main`).
