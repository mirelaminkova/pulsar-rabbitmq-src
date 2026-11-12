# Vendored byoc-sync contract

Verbatim copies from [sanjaysrikakulam/elixir_biohackathon_2026_project1](https://github.com/sanjaysrikakulam/elixir_biohackathon_2026_project1)
(`byoc-sync/src/byoc_sync/`) at commit `3b8bd72027a6f6bf751cfcee01b7ffecb25881a1`:

| File here | File in the byoc-sync repository |
| --- | --- |
| `naming.py` | `naming.py` (the RabbitMQ user, vhost, runner and destination names derived from the BYOC name) |
| `validate.py` | `validate.py` (the rules for the form's `username`, `password` and `nfs_path`) |
| `models.py` | `models.py` (the data types `validate.py` uses) |
| `eu.yml` | `sites/eu.yml` (usegalaxy.eu: the broker `mq.galaxyproject.eu:5671` and the naming patterns) |
| `templates/eu/runner.yml.j2` | `templates/eu/runner.yml.j2` (Galaxy's `PulsarMQJobRunner` runner for a BYOC Pulsar) |
| `templates/eu/destination.yml.j2` | `templates/eu/destination.yml.j2` (Galaxy's TPV destination for a BYOC Pulsar) |
| `templates/eu/mq_user.yml.j2` | `templates/eu/mq_user.yml.j2` (the RabbitMQ user and vhost) |

The tests feed this component's settings through these files, so the names, the validation
rules and Pulsar's `app.yml` are checked against byoc-sync's own code; the end-to-end test
(`tests/e2e/`) takes Galaxy's runner and destination parameters for this Pulsar from
`runner.yml.j2` and `destination.yml.j2`.
