# Pulsar for Galaxy's RabbitMQ on SURF Research Cloud

An SRC Ansible component that installs [Pulsar](https://github.com/galaxyproject/pulsar) as an AMQP **client** of the RabbitMQ broker that the Galaxy component [src-component-galaxy-amqp](https://github.com/mirelaminkova/src-component-galaxy-amqp) runs on the Galaxy workspace (`src_galaxy_rabbitmq: true`, `src_galaxy_pulsar_amqp_configs`). Supports Ubuntu 22.04 and 24.04.

No broker runs on this workspace. Pulsar connects *to* Galaxy's RabbitMQ over AMQPS, so this workspace needs no open inbound port; Galaxy's workspace must accept this workspace on its AMQPS port. Files go directly between Pulsar and Galaxy's HTTPS URL (`/api/`).

```
Pulsar (this workspace) ──AMQPS──▶ RabbitMQ on the Galaxy workspace (vhost /pulsar/<name>)
Pulsar ─────── HTTPS for input/output files ───────▶ https://<galaxy>/api/
```

The workspace root (`https://WORKSPACE_HOST/`) is an SRAM-protected **welcome page** with the matching `src_galaxy_pulsar_amqp_configs` entry for Galaxy and the state of this Pulsar.

## The Galaxy side is the contract

For each entry `<name>` of `src_galaxy_pulsar_amqp_configs`, the Galaxy component creates RabbitMQ user `<name>` (with the entry's `password`, or a generated one), vhost `/pulsar/<name>`, job runner `pulsar_mq_<name>` (`manager`, `amqp_acknowledge: true`, `amqp_ack_republish_time: 1200`, publish retries) and TPV environment `amqp_<name>` with the entry's `jobs_directory`. This component writes Pulsar's `app.yml` with exactly the settings of the Galaxy repository's `templates/rabbitmq/pulsar_amqp_app.yml.j2`, plus TLS verification (below). The tests prove this with Galaxy's own parser and template (vendored in [`tests/galaxy_contract/`](tests/galaxy_contract/README.md)).

## SRC catalog setup

1. Create the Galaxy workspace first, with `src_galaxy_rabbitmq: true` and an entry for this Pulsar, for example `src_galaxy_pulsar_amqp_configs: [{name: hpc, password: CHOOSE-A-LONG-PASSWORD, singularity_enabled: true}]`. Open `src_galaxy_rabbitmq_port` (default `5671`) in Galaxy's security group for this workspace. If you leave `password` out, Galaxy generates one and writes it to `/etc/galaxy-rabbitmq/pulsar_<name>.txt` on the Galaxy workspace.
2. Create an Ansible component using this repository and `playbook.yml` as the script. Install the roles and collections in `requirements.yml` through SRC's Ansible dependency mechanism, as for the [SRC Pulsar component](https://github.com/ErasmusMC-Bioinformatics/src-component-pulsar).
3. Add it to a catalog item using Ubuntu 22.04 or 24.04, **after SRC-Nginx** (for the SRAM-protected welcome page), with these component parameters. Each one is a value of Galaxy's entry for this Pulsar:

   | Parameter | Default | Galaxy side |
   | --- | --- | --- |
   | `pulsar_galaxy_fqdn` | required | Galaxy's `workspace_fqdn`, e.g. `galaxy.example.org` (no `https://`); must be the name in Galaxy's TLS certificate |
   | `pulsar_amqp_port` | `5671` | `src_galaxy_rabbitmq_port` |
   | `pulsar_amqp_name` | required | the entry's `name` (`a-z 0-9 _ -`, at most 64, not `galaxy`, `guest`, `true`, `false`) |
   | `pulsar_amqp_password` | required, secret | the entry's `password` (at least 12 of `A-Z a-z 0-9 . _ ~ -`) |
   | `pulsar_manager` | `_default_` | the entry's `manager` |
   | `pulsar_staging_dir` | `/mnt/pulsar/files/staging` | the entry's `jobs_directory` (Pulsar's `staging_directory`); at least two levels of `A-Z a-z 0-9 _ . -`, stricter than Galaxy because Pulsar owns it |
   | `pulsar_cvmfs` | `true` | mounts the Galaxy CVMFS repositories, as `src_galaxy_cvmfs` does on Galaxy |

   Empty values (how SRC passes an unset parameter) get these defaults. The playbook validates the parameters first, with the rules of Galaxy's parser, and lists every problem without echoing the password.
4. Create the workspace. Open `https://WORKSPACE_HOST/` (SRAM sign-in) for the Galaxy entry and status.

## What the playbook does

* Installs `pulsar-app[amqp]` 0.15.15 (kombu, py-amqp) in `/opt/pulsar/venv`, webless with `pulsar-main` in a `pulsar` systemd unit, as in usegalaxy.eu's [NEMO login-node role](https://github.com/usegalaxy-eu/pulsar-nemo-login-role). Jobs run on this workspace with the `queued_python` manager (vCPUs − 1 concurrent jobs).
* Writes `/opt/pulsar/config/app.yml` (`root:pulsar 0640`, the only file with the password besides the welcome page): `message_queue_url: pyamqp://<name>:<password>@<galaxy fqdn>:<port>//pulsar/<name>`, the same manager, `staging_directory`, `amqp_acknowledge: true`, `amqp_ack_republish_time: 1200` and publish retries as Galaxy's template.
* **Verifies Galaxy's TLS certificate and hostname**: `amqp_connect_ssl_cert_reqs: cert_required`, `amqp_connect_ssl_ca_certs: /etc/ssl/certs/ca-certificates.crt` (Ubuntu's CA bundle; SRC-Nginx certificates come from Let's Encrypt or ZeroSSL) and `amqp_connect_ssl_server_hostname: <galaxy fqdn>`.
  * The URL has no `?ssl=1`, unlike the Galaxy README's example: kombu lets the URL's `ssl` replace the `ssl` options Pulsar builds from `amqp_connect_ssl_*`, so with `?ssl=1` nothing beyond the CA chain would be checked. The `amqp_connect_ssl_*` settings switch TLS on by themselves.
  * py-amqp checks the hostname only when `server_hostname` is set, hence the third setting. `cert_reqs` takes the name of the `ssl` constant (`cert_required`); `required` would fail in Pulsar.
  * `tests/test_check_amqp.py` performs real TLS handshakes through Pulsar's connection code: a certificate for another host and one from an unknown CA are both rejected.
* **Checks the connection at deployment**: `/opt/pulsar/check_amqp.py` connects with `app.yml`'s settings through Pulsar's own code and passively declares Pulsar's setup queue (`pulsar__setup` for `_default_`, `pulsar_<manager>__setup` otherwise). A consumer on it proves that Pulsar logged in to its vhost and waits for jobs. The deployment fails with a clear message if the login is refused (name or password differs from Galaxy's entry), the vhost is not allowed, the certificate or hostname is rejected, the host does not resolve, or after two minutes the broker is unreachable (firewall, `src_galaxy_rabbitmq`) or Pulsar does not consume. Galaxy's broker has no management API for Pulsar's user, so this is the check. Run it again with `sudo /opt/pulsar/venv/bin/python /opt/pulsar/check_amqp.py /opt/pulsar/config/app.yml _default_`.
* Publishes the welcome page (`/srv/static/index.html`, readable by nginx only) before installing Pulsar, so the Galaxy entry is available even when the broker check fails.

### Containers and CVMFS

This Pulsar has no Conda, so a tool with requirements only runs in a container. Galaxy resolves the container image and builds the container command line; Pulsar only executes it, so the runtime and every path in that command line must exist here:

* Apptainer (`pulsar_apptainer`, default `true`) from the same PPA as the SRC Galaxy component, unprivileged (no setuid starter), with its `singularity` command. The welcome page's Galaxy entry then sets `singularity_enabled: true`.
* Docker Engine (`pulsar_docker`, default `true`) with the `pulsar` user in the `docker` group, which is root-equivalent on the host. The entry sets `docker_enabled: true` only without Apptainer: with both, Galaxy would prefer Docker, while Apptainer uses the CVMFS images.
* CVMFS (`pulsar_cvmfs`, default `true`): [galaxyproject.cvmfs](https://github.com/galaxyproject/ansible-cvmfs) 0.5.0 with the SRC Galaxy component's settings (`galaxyproject.org` repositories, 8 GB cache, European stratum 1 first), checked before Pulsar starts. `/cvmfs` is a system-wide Apptainer `bind path`, so tool data tables (`/cvmfs/data.galaxyproject.org/...`) and prebuilt images (`/cvmfs/singularity.galaxyproject.org/all/...`) resolve in jobs, with CVMFS on the Galaxy workspace too.

## Secrets

The password comes in as an SRC component parameter (mark it secret in the catalog). It is written only to `app.yml` (`0640`, group `pulsar`) and the SRAM-protected welcome page (`root:www-data 0640`). Tasks that template them use `no_log` and no diff; the validation and the broker check print no password, and Pulsar masks it in its own log. Anyone with root on this workspace or Galaxy's can read it.

To change it, change the entry's `password` on Galaxy, rerun the Galaxy component, then change `pulsar_amqp_password` and rerun this component.

## Testing

```bash
python3 -m pip install -r tests/requirements.txt
python3 -m unittest discover -s tests -t . -v
ansible-galaxy install -r requirements.yml
ansible-playbook -i localhost, playbook.yml --syntax-check
```

The tests cover parameter validation against Galaxy's parser, the welcome page's entry parsed by Galaxy's parser, `app.yml` against Galaxy's template and as Pulsar 0.15.15 reads it (user, vhost, URL, TLS options, queue names), real TLS handshakes for certificate and hostname verification, the broker check's messages, and the playbook's structure. `GALAXY_AMQP_REPO=/path/to/src-component-galaxy-amqp` also compares the vendored contract files with a Galaxy checkout; CI does this against the Galaxy repository's `main`.

A deployment against a real Galaxy and a job run (molecule, end to end) are not covered yet.

## Operations

```bash
systemctl status pulsar
journalctl -u pulsar -f
sudo systemctl restart pulsar
```

Pulsar keeps job state and message acknowledgement stores in `/var/lib/pulsar/persisted_data`. The staging directory holds job inputs and outputs; set `pulsar_staging_dir` (and Galaxy's `jobs_directory`) to attached storage for larger data. Workspace deletion or expiration deletes both.
