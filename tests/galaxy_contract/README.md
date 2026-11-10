# Vendored Galaxy contract

Verbatim copies from [mirelaminkova/src-component-galaxy-amqp](https://github.com/mirelaminkova/src-component-galaxy-amqp)
at commit `e13e0a807b9ba339f48a65d3e13d6e35bae8e1e2`:

| File here | File in the Galaxy repository |
| --- | --- |
| `src_galaxy.py` | `filter_plugins/src_galaxy.py` (parser and validation of `src_galaxy_pulsar_amqp_configs`) |
| `pulsar_amqp_app.yml.j2` | `templates/rabbitmq/pulsar_amqp_app.yml.j2` (the `app.yml` settings Galaxy expects from this Pulsar) |

The tests feed this component's output through these files, so the welcome page's
`src_galaxy_pulsar_amqp_configs` entry and Pulsar's `app.yml` are checked against the
Galaxy side's own code. Do not edit them; copy them again when the Galaxy repository
changes. `GALAXY_AMQP_REPO=/path/to/checkout python -m unittest tests.test_galaxy_contract`
compares them with a checkout (CI does this against the Galaxy repository's `main`).
