# Pulsar with RabbitMQ on SURF Research Cloud

An SRC Ansible component that installs [Pulsar](https://github.com/galaxyproject/pulsar) together with a [RabbitMQ](https://www.rabbitmq.com/) broker on one workspace, so a Galaxy server can send jobs to it over **AMQP**. Supports Ubuntu 22.04 and 24.04.

Galaxy publishes job messages to RabbitMQ on this workspace over AMQPS (TLS, port 5671); Pulsar, running on the same workspace, consumes them over `127.0.0.1` and publishes status updates back. Galaxy never connects to Pulsar directly and Pulsar needs no inbound port. **Files still transfer directly between Pulsar and Galaxy**: Pulsar must be able to reach Galaxy's HTTPS endpoint (`/api/`).

```
Galaxy ──AMQPS :5671──▶ nginx (workspace certificate) ──▶ RabbitMQ 127.0.0.1:5672 ◀── Pulsar
   ▲                                                                                  │
   └──────────────────────── HTTPS (input and output files) ─────────────────────────┘
```

As in the [SRC Pulsar component](https://github.com/ErasmusMC-Bioinformatics/src-component-pulsar), the workspace root (`https://WORKSPACE_HOST/`) is an SRAM-protected **welcome page** with copy-and-paste connection details: the `amqps://` URL with the generated account, a one-line parameter for the SRC Galaxy component, and a ready-made `job_conf.yml` snippet.

This repository started as a copy of [pulsar-relay-src](https://github.com/mirelaminkova/pulsar-relay-src); the relay, Valkey and the SRAM admin adapter are replaced by RabbitMQ, and Pulsar is always installed.

## SRC catalog setup

1. Create an Ansible component using this repository and `playbook.yml` as the script. Install the roles and collections in `requirements.yml` through SRC's Ansible dependency mechanism.
2. Add the component to a catalog item using Ubuntu 22.04 or 24.04. Include **SRC-Nginx before this component**, with a public workspace hostname and a valid HTTPS certificate. The playbook reuses that certificate for AMQPS (acme.sh at `/etc/ssl/acme/<fqdn>/`, or certbot's `/etc/letsencrypt/live/<fqdn>/` on older SRC-Nginx versions, detected as in the SRC Galaxy component).
3. **Open inbound TCP 5671** (or your `rabbitmq_amqps_port`) in the catalog item's firewall/security group, in addition to 443. If `ufw` is active on the workspace, the playbook adds the rule itself.
4. Provide these component parameters as needed:

   | Parameter | Default | Purpose |
   | --- | --- | --- |
   | `rabbitmq_hostname` | Workspace FQDN, falling back to `ansible_fqdn` | Hostname in the `amqps://` URL; must match the certificate |
   | `rabbitmq_pair_username` | `galaxy-pulsar` | RabbitMQ account shared by Galaxy and Pulsar |
   | `rabbitmq_pair_password` | empty: generated | Optional password, 16-128 of `A-Z a-z 0-9 . _ ~ -` (safe in a URL and in the Galaxy component's one-line parameter) |
   | `rabbitmq_vhost` | `pulsar` | RabbitMQ virtual host for Pulsar's queues |
   | `rabbitmq_amqps_port` | `5671` | Public AMQPS port |
   | `rabbitmq_management_ui` | `true` | RabbitMQ management UI at `https://WORKSPACE_HOST/rabbitmq/` (SRAM sign-in, then RabbitMQ login) |
   | `pulsar_manager` | `_default_` | Pulsar manager name; **Galaxy's runner must use the same** |
   | `pulsar_amqp_key_prefix` | empty: Pulsar's default | Queue name prefix; **Galaxy's runner must use the same** |
   | `pulsar_amqp_acknowledge` | `false` | Message acknowledgement; **on both Galaxy and Pulsar, or on neither** |
   | `pulsar_cvmfs` | `true` | Mount the Galaxy project's CVMFS repositories (reference data and Singularity images) |
   | `pulsar_root` | empty: `/var/lib/pulsar` | Pulsar data directory; set to attached storage for larger data |

5. Create the workspace. Start with at least 2 vCPUs and 4 GB RAM; jobs run on this workspace, so size it for the tools you send.
6. Open `https://WORKSPACE_HOST/` (SRAM sign-in) for the Galaxy configuration.

The playbook installs Docker Engine and Compose v2, starts RabbitMQ (`rabbitmq:4.1-management`) from `compose.yml` in `/opt/pulsar-rabbitmq`, adds an nginx `stream` listener that terminates TLS on 5671 and forwards to RabbitMQ on `127.0.0.1:5672`, installs Pulsar in `/opt/pulsar/venv` with a `pulsar` systemd unit, and finally waits until Pulsar consumes its setup queue, which proves that Pulsar logged in to RabbitMQ.

## Galaxy compatibility

Galaxy's `PulsarMQJobRunner` and Pulsar build the same `PulsarExchange` (`pulsar.client.amqp_exchange`): a durable **direct exchange named `pulsar`** with one durable queue per message type, named `<key prefix>_<type>`. The key prefix is `amqp_key_prefix` when set, otherwise `pulsar_` for the `_default_` manager and `pulsar_<manager>_` for any other. With the defaults:

| Queue | Published by | Consumed by |
| --- | --- | --- |
| `pulsar__setup` | Galaxy (new job) | Pulsar |
| `pulsar__kill` | Galaxy | Pulsar |
| `pulsar__status` | Galaxy | Pulsar |
| `pulsar__status_update` | Pulsar | Galaxy job handler |

Publishers declare only the exchange, so a message sent to a queue that does not exist yet is dropped. This component therefore **pre-declares the exchange, the queues and their bindings** in RabbitMQ's definitions (with the properties kombu uses, so the clients' own declarations match). A status update sent before Galaxy first connects, or a job sent while Pulsar restarts, waits in its queue. `tests/test_pulsar_compat.py` checks these names against the installed Pulsar code.

For a Galaxy (SRC component or hand-made) to work with this workspace, its Pulsar runner needs:

* `load: galaxy.jobs.runners.pulsar:PulsarMQJobRunner` with `amqp_url: amqps://USER:PASSWORD@WORKSPACE_HOST:5671/VHOST` (the welcome page prints it). Use `amqps`, not `amqp`: port 5671 only speaks TLS. The certificate is a public one, so no `amqp_connect_ssl_*` options are needed.
* the same `manager` (on the runner), `amqp_key_prefix` and `amqp_acknowledge` as this workspace;
* `galaxy_url` on the runner set to Galaxy's public URL (otherwise `galaxy_infrastructure_url`), with Galaxy's API reachable from this workspace;
* on the environment: `jobs_directory: /var/lib/pulsar/staging` (or `<pulsar_root>/staging`), `default_file_action: remote_transfer`, `dependency_resolution: remote`. Galaxy ignores `url` and `private_token` for this runner;
* **a single job handler** for these jobs. RabbitMQ delivers each message on `pulsar__status_update` to exactly one consumer; with several handlers, one would receive updates for jobs another handler owns. The SRC Galaxy component already runs one dedicated handler in relay mode; its AMQP mode needs the same.
* containers: this Pulsar has no Conda, so tools with requirements need `singularity_enabled: true` (Apptainer, with `/cvmfs` bound into every container) or `docker_enabled: true`.

Galaxy's own `amqp_internal_connection` in `galaxy.yml` (Galaxy's internal control messages) is unrelated; it can stay on PostgreSQL.

### SRC Galaxy component parameter

The welcome page prints a one-line list for a Galaxy component parameter `src_galaxy_pulsar_amqp_configs`, in the same style as `src_galaxy_pulsar_relay_configs`:

```
[{name: NAME, amqp_url: amqps://galaxy-pulsar:PASSWORD@WORKSPACE_HOST:5671/pulsar, manager: _default_, jobs_directory: /var/lib/pulsar/staging, singularity_enabled: true}]
```

`amqp_key_prefix: ...` and `amqp_acknowledge: true` are added when set here. Tool mappings use `environment: remote_NAME` as for the relay. Generated passwords are 64 hex characters, so the values contain no commas, colons or quotes and need no URL encoding.

### Hand-edited Galaxy

```yaml
runners:
  pulsar_amqp:
    load: galaxy.jobs.runners.pulsar:PulsarMQJobRunner
    amqp_url: amqps://galaxy-pulsar:PASSWORD@WORKSPACE_HOST:5671/pulsar
    manager: _default_
    galaxy_url: https://GALAXY_HOST

execution:
  environments:
    remote_pulsar:
      runner: pulsar_amqp
      jobs_directory: /var/lib/pulsar/staging
      dependency_resolution: remote
      default_file_action: remote_transfer
      singularity_enabled: true
      singularity_volumes: "$job_directory:ro,$tool_directory:ro,$job_directory/outputs:rw,$working_directory:rw,/cvmfs:ro"
      container_resolvers:
        - type: explicit_singularity
        - type: cached_mulled_singularity
          cache_directory: /cvmfs/singularity.galaxyproject.org/all
        - type: mulled_singularity
          auto_install: true

tools:
  - id: YOUR-TOOL-ID
    environment: remote_pulsar
```

Protect the file (it contains the password) and restart Galaxy (`systemctl restart 'galaxy-*'`).

## Pulsar on this workspace

`pulsar-app[amqp]` 0.15.15 (the extra installs kombu) runs webless with `pulsar-main` from a `pulsar` systemd unit. Its `app.yml` (`/opt/pulsar/config/app.yml`, mode `0640`) is written from the generated credentials: `message_queue_url: amqp://galaxy-pulsar:...@127.0.0.1:5672/pulsar`, the manager, key prefix and acknowledgement setting, and publish retries so status updates survive a broker restart. Jobs run on the workspace with the `queued_python` manager (CPUs − 1 concurrent jobs). Staging lives in `/var/lib/pulsar/staging`.

* `pulsar_docker` (default `true`) adds the `pulsar` user to the `docker` group so Galaxy environments can set `docker_enabled: true`; that group is root-equivalent on the host.
* `pulsar_apptainer` (default `true`) installs Apptainer from the same PPA as the SRC Galaxy component, unprivileged, with its `singularity` command.
* `pulsar_cvmfs` runs [galaxyproject.cvmfs](https://github.com/galaxyproject/ansible-cvmfs) 0.5.0 as a client with the same settings as the SRC Galaxy component's `src_galaxy_cvmfs`, checks that `data.galaxyproject.org` and `singularity.galaxyproject.org` mount, and binds `/cvmfs` into every Apptainer container.

## Credentials and security

`scripts/configure.py` generates the pair and admin passwords once in `/opt/pulsar-rabbitmq/secrets.json` (mode `0600`) and keeps them across reruns. RabbitMQ receives only salted SHA-256 password hashes in `.secrets/definitions.json`, imported at every broker start together with the vhost, permissions, exchange and queues. The default `guest` account is not created.

* The pair account has full permissions on the Pulsar vhost only and no management access.
* The admin account (`administrator` tag) is for the management UI, served at `/rabbitmq/` behind SRAM sign-in. Its password is on the welcome page, so every collaboration member who can sign in can use it; set `rabbitmq_management_ui: false` to remove the UI location.
* Plain AMQP (5672) and the management port (15672) are bound to `127.0.0.1`. Only nginx's 443 and 5671 are reachable externally.

To rotate the pair password, set `rabbitmq_pair_password` and rerun the component (or edit `secrets.json` and rerun it). The broker is recreated with the new hash, Pulsar's `app.yml` is rewritten and Pulsar restarted, and the welcome page shows the new URL; then update Galaxy.

## Local development and testing

```bash
python3 -m pip install 'pulsar-app[amqp]==0.15.15'    # for the compatibility tests and the live test
python3 -m unittest discover -s tests -v
ansible-galaxy install -r requirements.yml
ansible-playbook -i localhost, playbook.yml --syntax-check
sudo bash scripts/install_docker.sh                     # if Docker is missing
python3 scripts/test_rabbitmq.py
```

The unit tests check credential retention, file permissions, password hashes, input validation and the generated definitions; `test_pulsar_compat.py` checks the pre-declared queue names against Pulsar's own `PulsarExchange` (skipped if Pulsar is not installed). The live test starts RabbitMQ in a disposable Compose project and, with Pulsar's exchange code on both sides, sends a job message before any consumer exists, returns a status update, checks that the pair and guest accounts have no management access, and checks that a queued message survives container recreation. It does not run a Galaxy job, nor test SRC's TLS/SRAM setup; validate those on SRC with a real Galaxy.

For a manual installation on an SRC workspace:

```bash
ansible-galaxy install -r requirements.yml
ansible-playbook -i localhost, -c local playbook.yml --become
```

For a non-SRC host, `-e rabbitmq_configure_src_proxy=false` skips SRC-Nginx, the AMQPS listener and the welcome page (Pulsar still connects locally).

## Operations

```bash
cd /opt/pulsar-rabbitmq
sudo docker compose -p pulsar-rabbitmq ps
sudo docker compose -p pulsar-rabbitmq logs --tail 100 rabbitmq
sudo docker compose -p pulsar-rabbitmq exec rabbitmq rabbitmqctl list_queues -p pulsar name messages consumers
systemctl status pulsar; journalctl -u pulsar -f
```

RabbitMQ keeps queues and messages in the named volume `pulsar-rabbitmq_rabbitmq-data`; the container's fixed hostname keeps the node name, and so the data, across recreation. **Workspace deletion or expiration deletes it**, together with Pulsar's staging data. Do not run `docker compose down --volumes` on the production project. `consumer_timeout` is raised to one hour (`rabbitmq_consumer_timeout_ms`).
