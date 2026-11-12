# Pulsar for Galaxy's RabbitMQ on SURF Research Cloud

An SRC Ansible component that installs [Pulsar](https://github.com/galaxyproject/pulsar) as an AMQP **client** of the RabbitMQ broker that the Galaxy component [src-component-galaxy-amqp](https://github.com/mirelaminkova/src-component-galaxy-amqp) runs on the Galaxy workspace (`src_galaxy_rabbitmq: true`, `src_galaxy_pulsar_amqp_configs`), or of a broker on another host such as usegalaxy.eu's [Pulsar Network](#usegalaxyeus-pulsar-network) (`pulsar_amqp_host`). Supports Ubuntu 22.04 and 24.04.

No broker runs on this workspace. Pulsar connects *to* Galaxy's RabbitMQ over AMQPS, so this workspace needs no open inbound port; the broker's workspace must accept this workspace on its AMQPS port. Files go directly between Pulsar and Galaxy's HTTPS URL (`/api/`).

```
Pulsar (this workspace) ──AMQPS──▶ RabbitMQ on the Galaxy workspace, or pulsar_amqp_host (vhost /pulsar/<name>)
Pulsar ─────── HTTPS for input/output files ───────▶ https://<galaxy>/api/
```

The workspace root (`https://WORKSPACE_HOST/`) is an SRAM-protected **welcome page** with the matching `src_galaxy_pulsar_amqp_configs` entry for Galaxy and the state of this Pulsar.

## The Galaxy side is the contract

For each entry `<name>` of `src_galaxy_pulsar_amqp_configs`, the Galaxy component creates RabbitMQ user `<name>` (with the entry's `password`, or a generated one), vhost `/pulsar/<name>`, job runner `pulsar_mq_<name>` (`manager`, `amqp_acknowledge: true`, `amqp_ack_republish_time: 1200`, publish retries) and TPV environment `amqp_<name>` with the entry's `jobs_directory`. This component writes Pulsar's `app.yml` with the settings of the Galaxy repository's `templates/rabbitmq/pulsar_amqp_app.yml.j2`, except that the URL has no `?ssl=1` (replaced by TLS verification, below) and the manager's type: Galaxy's runner only uses the manager's name, so this Pulsar runs its jobs through Slurm (`queued_drmaa`, below) or, with `pulsar_job_runner: local`, itself with vCPUs − 1 concurrent jobs instead of 1. The tests prove this with Galaxy's own parser and template (vendored in [`tests/galaxy_contract/`](tests/galaxy_contract/README.md)).

## SRC catalog setup

1. Create a storage volume in SRC and attach it to this workspace when you create it (SRC mounts it at `/data/<volume name>`). Pulsar keeps all its data there.
2. Create the Galaxy workspace first, with `src_galaxy_rabbitmq: true` and an entry for this Pulsar, for example `src_galaxy_pulsar_amqp_configs: [{name: hpc, password: CHOOSE-A-LONG-PASSWORD, jobs_directory: /data/pulsar_storage/pulsar/staging, singularity_enabled: true}]` (or copy the entry from this workspace's welcome page afterwards and rerun the Galaxy component). Open `src_galaxy_rabbitmq_port` (default `5671`) in Galaxy's security group for this workspace. If you leave `password` out, Galaxy generates one and writes it to `/etc/galaxy-rabbitmq/pulsar_<name>.txt` on the Galaxy workspace.
3. Create an Ansible component using this repository and `playbook.yml` as the script. Install the roles and collections in `requirements.yml` through SRC's Ansible dependency mechanism, as for the [SRC Pulsar component](https://github.com/ErasmusMC-Bioinformatics/src-component-pulsar).
4. Add it to a catalog item using Ubuntu 22.04 or 24.04, **after SRC-Nginx** (for the SRAM-protected welcome page), with these component parameters. Each one is a value of Galaxy's entry for this Pulsar:

   | Parameter | Default | Galaxy side |
   | --- | --- | --- |
   | `pulsar_galaxy_fqdn` | required | Galaxy's `workspace_fqdn`, e.g. `galaxy.example.org` (no `https://`): shown on the welcome page, and the broker (the name in its TLS certificate) unless `pulsar_amqp_host` is set |
   | `pulsar_amqp_host` | empty | none: the RabbitMQ hostname when the broker is not the Galaxy workspace (the name in its TLS certificate), e.g. `mq.galaxyproject.eu` for [usegalaxy.eu](#usegalaxyeus-pulsar-network); empty means `pulsar_galaxy_fqdn` |
   | `pulsar_amqp_port` | `5671` | `src_galaxy_rabbitmq_port` (not 5672, 4369, 25672, 80, 443 or 8080, as on Galaxy) |
   | `pulsar_amqp_name` | required | the entry's `name` (`a-z 0-9 _ -`, at most 64, not `galaxy`, `guest`, `true`, `false`) |
   | `pulsar_amqp_password` | required, secret | the entry's `password` (at least 12 of `A-Z a-z 0-9 . _ ~ -`) |
   | `pulsar_manager` | `_default_` | the entry's `manager` |
   | `pulsar_storage_path` | required | the attached volume's mount point `/data/<volume name>` (Pulsar uses its `pulsar` directory) or a directory on it (`A-Z a-z 0-9 _ . -`); the entry's `jobs_directory` is `<storage directory>/staging`. See [Attached storage](#attached-storage) |
   | `pulsar_apptainer` | `true` | the entry's `singularity_enabled` |
   | `pulsar_docker` | `false` | the entry's `docker_enabled` (only without Apptainer) |
   | `pulsar_cvmfs` | `true` | mounts the Galaxy CVMFS repositories, as `src_galaxy_cvmfs` does on Galaxy |
   | `pulsar_job_runner` | `slurm` | none: `slurm` runs jobs through Slurm on this workspace (DRMAA), `local` in Pulsar itself |
   | `pulsar_amqp_ack_republish_time` | `1200` | the runner's `amqp_ack_republish_time`: seconds before an unacknowledged message is sent again; `1200` for the Galaxy component's runner, `300` for usegalaxy.eu's |

   Empty values (how SRC passes an unset parameter) get these defaults, and an empty required parameter is an error; a misspelt `true`/`false` or runner is an error. The playbook validates the parameters first, with the rules of Galaxy's parser, and lists every problem without echoing the password.
5. Create the workspace with the storage volume attached. Open `https://WORKSPACE_HOST/` (SRAM sign-in) for the Galaxy entry and status.

### usegalaxy.eu's Pulsar Network

[usegalaxy.eu's Pulsar Network](https://pulsar-network.readthedocs.io/en/latest/rabbitmq.html) uses the same user and vhost convention (`galaxy_<id>`, `/pulsar/galaxy_<id>`), but its RabbitMQ is a separate host, `mq.galaxyproject.eu`, not the Galaxy server: Galaxy's runner `pulsar_eu_<id>` connects with `pyamqp://galaxy_<id>:<password>@mq.galaxyproject.eu:5671//pulsar/galaxy_<id>?ssl=1` (usegalaxy-eu/infrastructure-playbook, `templates/galaxy/config/job_conf.yml`), `manager: production` and `amqp_ack_republish_time: 300`. After the usegalaxy.eu admins have created your user and vhost and given you the password, set:

| Parameter | Value |
| --- | --- |
| `pulsar_galaxy_fqdn` | `usegalaxy.eu` (named on the welcome page only; Galaxy's runner tells Pulsar its URL) |
| `pulsar_amqp_host` | `mq.galaxyproject.eu` (the broker; its certificate is verified for this name) |
| `pulsar_amqp_name` | `galaxy_<id>` |
| `pulsar_amqp_password` | the password you were given |
| `pulsar_manager` | `production` |
| `pulsar_amqp_ack_republish_time` | `300` |

The welcome page's `src_galaxy_pulsar_amqp_configs` entry is then not needed (usegalaxy.eu configures its runner itself, including `galaxy_url`, where Pulsar fetches inputs and posts outputs); the AMQP URL in `app.yml` is usegalaxy.eu's without `?ssl=1`, which the `amqp_connect_ssl_*` settings replace (below). Do not paste that URL into `pulsar_amqp_host`: only the hostname goes there, and the validation never echoes a value with a login.

## What the playbook does

* Installs `pulsar-app[amqp]` 0.15.15 (kombu, py-amqp) and `drmaa` in `/opt/pulsar/venv`, webless with `pulsar-main` in a `pulsar` systemd unit, as in usegalaxy.eu's [NEMO login-node role](https://github.com/usegalaxy-eu/pulsar-nemo-login-role). Jobs run on this workspace through Slurm (below), or with `pulsar_job_runner: local` in Pulsar itself (`queued_python`, vCPUs − 1 concurrent jobs).
* Writes `/opt/pulsar/config/app.yml` (`root:pulsar 0640`, the only file with the password besides the welcome page): `message_queue_url: pyamqp://<name>:<password>@<broker host>:<port>//pulsar/<name>` (the broker host is `pulsar_amqp_host`, or `pulsar_galaxy_fqdn` when it is empty), the same manager, `staging_directory`, `amqp_acknowledge: true`, `amqp_ack_republish_time: <pulsar_amqp_ack_republish_time>` (`1200`, as Galaxy's template) and publish retries as Galaxy's template.
* **Verifies the broker's TLS certificate and hostname**: `amqp_connect_ssl_cert_reqs: cert_required`, `amqp_connect_ssl_ca_certs: /etc/ssl/certs/ca-certificates.crt` (Ubuntu's CA bundle; SRC-Nginx certificates come from Let's Encrypt or ZeroSSL) and `amqp_connect_ssl_server_hostname: <broker host>`.
  * The URL has no `?ssl=1`, unlike the Galaxy README's example: kombu lets the URL's `ssl` replace the `ssl` options Pulsar builds from `amqp_connect_ssl_*`, so with `?ssl=1` nothing beyond the CA chain would be checked. The `amqp_connect_ssl_*` settings switch TLS on by themselves.
  * py-amqp checks the hostname only when `server_hostname` is set, hence the third setting. `cert_reqs` takes the name of the `ssl` constant (`cert_required`); `required` would fail in Pulsar.
  * `tests/test_check_amqp.py` performs real TLS handshakes through Pulsar's connection code: a certificate for another host and one from an unknown CA are both rejected.
* **Checks the connection at deployment**: `/opt/pulsar/check_amqp.py` connects with `app.yml`'s settings through Pulsar's own code and passively declares Pulsar's setup queue (`pulsar__setup` for `_default_`, `pulsar_<manager>__setup` otherwise). A consumer on it proves that Pulsar logged in to its vhost and waits for jobs. The deployment fails with a clear message if the login is refused (name or password differs from Galaxy's entry), the vhost is not allowed, the certificate or hostname is rejected, the host does not resolve, or, after 24 attempts five seconds apart (about two minutes; up to about six when a firewall drops packets, since each attempt then waits for connection timeouts), the broker is unreachable (firewall, `src_galaxy_rabbitmq`) or Pulsar does not consume. Galaxy's broker has no management API for Pulsar's user, so this is the check. Run it again with `sudo /opt/pulsar/venv/bin/python /opt/pulsar/check_amqp.py /opt/pulsar/config/app.yml` (exit status 0 consuming, 1 configuration error, 75 not yet). A consumer can also be a half-open connection of an earlier Pulsar with the same name and password, until its heartbeat times out (up to about 20 minutes); give every Pulsar its own entry.
* **Checks the attached storage first**, before installing anything, with `files/check_storage.py`, and refuses a `pulsar_storage_path` that cannot safely hold Pulsar's data; see [Attached storage](#attached-storage) for the rules. A volume root that other users can write to gets the sticky bit. The same check runs before every start of Pulsar, Docker and slurmd.
* `force_handlers` restarts Pulsar after a changed `app.yml` or venv even when a later task fails, so a rerun cannot leave the old Pulsar running.
* Publishes the welcome page (`/srv/static/index.html`, readable by nginx only) before installing Pulsar, so the Galaxy entry is available even when the broker check fails.

### Jobs through Slurm

With `pulsar_job_runner: slurm` (the default), as in the [earlier SRC Pulsar component](https://github.com/ErasmusMC-Bioinformatics/src-component-pulsar/tree/pulsar_attached), Pulsar's manager is `queued_drmaa`: it submits each job through the `slurm-drmaa1` library (`DRMAA_LIBRARY_PATH=/usr/lib/slurm-drmaa/lib/libdrmaa.so.1` in the `pulsar` unit; from the Galaxy project's [slurm-drmaa PPA](https://launchpad.net/~natefoo/+archive/ubuntu/slurm-drmaa), which `galaxyproject.repos` also adds, since Ubuntu has had no slurm-drmaa since 16.04) to a single-node Slurm on this workspace. Galaxy sees no difference: same entry, manager name and queues.

* The signing keys of the slurm-drmaa and Apptainer PPAs ship in this repository (`files/slurm-drmaa.asc`, `files/apptainer.asc`; the tests check their Launchpad fingerprints `8DE68488…12788738` and `F6B0F519…FC6218AE`): a keyserver's export of a key changes between downloads, so downloading it made every rerun report a change. When a PPA rotates its key, replace the file (CI's weekly Slurm live check shows it). An apt preference lets the slurm-drmaa PPA supply only `slurm-drmaa*` packages. The PPA builds amd64 only, so the Slurm runner needs an x86_64 workspace. Another playbook on the same workspace that adds these PPAs without `signed-by` (such as `galaxyproject.repos`) makes apt refuse the conflicting `Signed-By`.
* [galaxyproject.slurm](https://github.com/galaxyproject/ansible-slurm) 1.0.5 with `slurm_roles: [controller, exec]`: this workspace is controller and only node (`localhost`, partition `pulsar`), so no shared filesystem is needed. munge's key is the random one the package generates on this workspace; the playbook neither ships nor reads it, and checks only that it exists and is private to `munge`.
* **Node size from the workspace**: `CPUs` = vCPUs; `RealMemory` = memory minus `pulsar_slurm_reserved_memory_mb` (at least 1 GiB or a tenth, never more than half) for the OS, Pulsar and its broker connection. Slurm drains a node that has less than its configured memory, so the node is never configured larger than the workspace. `select/cons_tres` with `CR_CPU_Memory` lets jobs share the node (Ubuntu 22.04's Slurm would otherwise give each job the whole node), and `DefMemPerCPU` gives a job without a memory request its CPU's share, so every CPU can run a job. Galaxy can still request more per destination (`submit_native_specification`). Memory is not enforced (no cgroup task plugin, `proctrack/pgid`): the numbers only decide how many jobs start, and a job that uses more than its share can still push the workspace into the OOM killer, as with the local runner.
* Jobs run as the `pulsar` user: DRMAA submits as Pulsar's user, and slurmd starts the job as that user. Unlike with the local runner, jobs run outside the `pulsar` unit and so without its sandboxing (`ProtectHome`, `PrivateTmp`, `NoNewPrivileges`): a tool can read world-readable files in `/home` and use the real `/tmp` (only `TMPDIR` points to the storage). Slurm's `TaskProlog` (`/etc/slurm/task_prolog.sh`) gives each job the storage's home, `TMPDIR` and Apptainer directories (Pulsar's own jobs get their home and `TMPDIR` from the unit and keep Apptainer's cache under `~`). Apptainer and Docker work as with the local runner.
* `slurmctld` (6817) and `slurmd` (6818) listen on all interfaces, as Slurm always does; every request is authenticated with this workspace's own munge key. Keep these ports closed to other hosts in the workspace's security group. Slurm's `NoInAddrAny` would bind them to the hostname's address, not to localhost, and break the node's `localhost` address.
* **Docker epilog** (`pulsar_docker: true` only, with `jq`): cancelling a job kills its processes, not the Docker container it started, which runs under dockerd. Slurm's `Epilog` [`files/epilog.sh`](files/epilog.sh), after usegalaxy.org's, runs as root after every job and `docker kill`s the container named in the job's `configs/container_config.json` (Galaxy writes it). That file is the job's, so the epilog reads it with the job user's rights, only as a regular file, with time limits, and kills only a plain container name; it always succeeds, because a failing epilog drains the node. `PrologEpilogTimeout: 90`.
* **On the storage**: job working directories are in staging; slurmd's spool (`SlurmdSpoolDir`) and each job's temporary directory (`TMPDIR`, `<storage directory>/job_tmp/<job id>`, removed by Slurm's `TaskEpilog` when the job script ends; `TmpFS`) are on the storage too, and so is the `pulsar` user's home. Slurm's controller state (`StateSaveLocation`, `/var/lib/slurm/slurmctld`, which keeps each batch script for `MinJobAge`) and logs stay on the root disk. `slurmd`, like Docker, starts only when the storage check passes.
* The deployment waits until the node accepts jobs (`sinfo`) and fails with `scontrol show node localhost` otherwise. `ReturnToService: 2` brings it back by itself after a reboot.
* Slurm accounting is off (no `slurmdbd`), so there is no `sacct`. Inspect jobs with `squeue` (queued and running), `scontrol show job <id>` (also for four hours after the end, `MinJobAge`), `sinfo` (the node) and `scancel <id>`; Pulsar's log has every job. Slurm's logs are in `/var/log/slurm/`.
* Switching to `local` stops and disables `slurmctld` and `slurmd` (Slurm stays installed); switching back starts them again. Pulsar restarts on either switch. Switch when no jobs are active: Pulsar cannot move jobs between managers and reports them to Galaxy as lost.

### Containers and CVMFS

This Pulsar has no Conda, so a tool with requirements only runs in a container. Galaxy resolves the container image and builds the container command line; Pulsar only executes it, so the runtime and every path in that command line must exist here:

* Apptainer (`pulsar_apptainer`, default `true`) from the same PPA as the SRC Galaxy component, unprivileged (no setuid starter), with its `singularity` command. The welcome page's Galaxy entry then sets `singularity_enabled: true`. Galaxy's container resolvers (its global `container_resolvers_conf.yml`; AMQP destinations set none) give `docker://quay.io/...` images, which Apptainer pulls here on first use (outbound HTTPS needed; the first run of a tool is slow).
* Docker Engine (`pulsar_docker`, default `false`) with the `pulsar` user in the `docker` group, which is root-equivalent on the host. The entry sets `docker_enabled: true` only without Apptainer: with both, Galaxy would prefer Docker.
* CVMFS (`pulsar_cvmfs`, default `true`): [galaxyproject.cvmfs](https://github.com/galaxyproject/ansible-cvmfs) 0.5.0 with the SRC Galaxy component's settings (`galaxyproject.org` repositories, 8 GB cache, European stratum 1 first), checked before Pulsar starts. `/cvmfs` is a system-wide Apptainer `bind path`, so tool data table paths (`/cvmfs/data.galaxyproject.org/...`) resolve in jobs, with CVMFS on the Galaxy workspace too.

## Secrets

The password comes in as an SRC component parameter (mark it secret in the catalog). It is written only to `app.yml` (`0640`, group `pulsar`) and the SRAM-protected welcome page (`root:www-data 0640`). Tasks that template them use `no_log` and no diff; the validation and the broker check print no password, and Pulsar masks it in its own log. Anyone with root on this workspace or Galaxy's can read it.

To change it, change the entry's `password` on Galaxy, rerun the Galaxy component, then change `pulsar_amqp_password` and rerun this component.

## Testing

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
. .venv/bin/activate
ansible-galaxy install -r requirements.yml   # before pytest: it renders galaxyproject.slurm's slurm.conf
pytest                                  # 100% branch coverage
ansible-playbook -i localhost, playbook.yml --syntax-check
```

The git hooks run the same checks as CI (`.github/workflows/checks.yml`):

```bash
pre-commit install                      # pre-commit and pre-push hooks
pre-commit run --all-files              # yamllint --strict, ruff, ansible-lint, pytest, ast-grep test
pre-commit run --hook-stage pre-push --all-files   # semgrep (p/default, p/secrets), ast-grep scan
```

The ast-grep rules in `ast-grep/rules/` (tests in `ast-grep/rule-tests/`) refuse `eval`/`exec` and the usual ways of switching off TLS certificate or hostname verification, in Python code and in `amqp_connect_ssl_cert_reqs` of the app.yml template; the pytest tests check the rendered TLS settings themselves. `tests/galaxy_contract/` is vendored from the Galaxy component and excluded from linting, formatting and coverage.

The tests cover parameter validation against Galaxy's parser, the welcome page's entry parsed by Galaxy's parser, `app.yml` against Galaxy's template and as Pulsar 0.15.15 reads it (user, vhost, URL, TLS options, queue names), real TLS handshakes for certificate and hostname verification, the broker check's messages, the playbook's structure, the Slurm settings (rendered with galaxyproject.slurm's own `slurm.conf` template, so install the roles first), the node sizing, the job runner parameter, the epilog against stand-ins for `scontrol`, `jq` and `docker`, and Pulsar building its manager from `app.yml`. `GALAXY_AMQP_REPO=/path/to/src-component-galaxy-amqp` also compares the vendored contract files with a Galaxy checkout; CI does this against the Galaxy repository's `main` in a separate job, so a change on the Galaxy side shows up as a failed contract check without hiding this repository's own results.

CI's Slurm live check ([`.github/workflows/slurm-live.yml`](.github/workflows/slurm-live.yml)) runs the playbook on GitHub's Ubuntu 22.04 and 24.04 runners without a Galaxy broker (`--skip-tags broker-check,docker-install`), twice to check that a rerun changes nothing, and then runs jobs as the `pulsar` user through Pulsar's own `queued_drmaa` manager from the deployed venv and `app.yml` ([`tests/live/pulsar_job.py`](tests/live/pulsar_job.py)): one that reports its user and Slurm job, one in Apptainer, one in Docker, and one whose container the epilog must stop. It has no broker: the run from Galaxy is in the molecule test below.

Both CI jobs run the playbook twice over: with `ansible==14.6.0`, the `remote_ansible_version` SRC's external Ansible plugin runs it with (ansible-core 2.21), and with `requirements-dev.txt`'s ansible-core 2.18. The unit tests stay on 2.18: [`tests/helpers.py`](tests/helpers.py) renders templates with Templar methods that ansible-core 2.19 removed, so the molecule job runs `tests/e2e/galaxy_config.py` with a separate `.venv` of `requirements-dev.txt` (`MOLECULE_TESTS_PYTHON`). The playbook reads facts through `ansible_facts[...]` (a test checks it): ansible-core 2.21 warns about top-level fact variables such as `ansible_distribution`, which 2.24 no longer sets. The roles from `requirements.yml` still use them.

### Integration and end-to-end tests (molecule)

CI's molecule job ([`.github/workflows/molecule.yml`](.github/workflows/molecule.yml), scenario [`molecule/default`](molecule/default)) needs Docker and runs on GitHub's runners only. It starts two SRC test workspaces (`ghcr.io/utrechtuniversity/src-test-workspace`) on one Docker network, privileged and with systemd; it is modelled on the Galaxy component's molecule test, with Docker instead of podman for the privileges Slurm and the volume need:

* `galaxy` (Ubuntu 24.04), alias `galaxy.molecule.test`: RabbitMQ set up as the Galaxy component sets it up (Ubuntu's package, its `rabbitmq.conf` from [`tests/galaxy_contract/`](tests/galaxy_contract/README.md), node `rabbit@localhost`, no `guest`), with a certificate for `galaxy.molecule.test` from a test CA, vhost `/pulsar/molecule`, user `molecule` and Galaxy's own user `galaxy`;
* `pulsar` (Ubuntu 24.04, and 22.04 in a second job): this playbook with `pulsar_job_runner: slurm` and `pulsar_storage_path: /data/molecule/pulsar` on a separate ext4 volume of the runner. The test CA goes into the system CA bundle, as a public CA is there on SRC, so Pulsar's certificate and hostname verification stay on.

The playbook runs twice (the second run must change nothing). The verify step then checks that `check_amqp.py` passes, that Pulsar's `app.yml` verifies the certificate and hostname (and that the same settings with a wrong hostname are refused), that Pulsar, munge, slurmctld and slurmd are active and the Slurm node takes jobs, that Pulsar's data directories (staging, persisted data, home, job temporary files, slurmd's spool, Apptainer's directories) are on the storage mount, and, on the broker, that a consumer of `pulsar__setup` on `/pulsar/molecule` exists and `molecule` is connected over TLS.

The end-to-end test then submits a job the way Galaxy does. [`tests/e2e/galaxy_config.py`](tests/e2e/galaxy_config.py) derives the welcome page's entry from the component parameters and renders Galaxy's `PulsarMQJobRunner` runner and TPV destination for it from the Galaxy component's own `job_conf.yml.j2` and `destinations.yml.j2` (vendored, checked against Galaxy's `main` by the contract job). On the Galaxy workspace, [`tests/e2e/galaxy_job.py`](tests/e2e/galaxy_job.py) does with Pulsar's client library as Galaxy 26.1 pins it (`pulsar-galaxy-lib`, [`tests/e2e/requirements-galaxy.txt`](tests/e2e/requirements-galaxy.txt)) what Galaxy's runner does: it builds the client manager from the runner's parameters, consumes status updates and acknowledgements, lays out the job with the destination's parameters (`remote_transfer`, `rewrite_parameters`), submits a small job (sort a file, report user, Slurm job and working directory) and waits for its final status. A small HTTP server stands in for Galaxy's job files API at `galaxy_url`: Pulsar fetches the input from it and posts the outputs back. The test passes when the job completed with exit code 0 after a `running` status update, both outputs arrived, and the job ran as `pulsar` in a Slurm job in `/data/molecule/pulsar/staging/<job>/`. The unit tests run the same client against an in-process Pulsar on kombu's in-memory transport. The test does not exercise Galaxy's tool, dependency or container handling, so the destination's `singularity_enabled`, `remote_metadata` and `outputs_to_working_directory` play no part in it.

To run it elsewhere, install `requirements-dev.txt`, `molecule/requirements.txt`, `requirements.yml` and `molecule/default/requirements.yml`, mount an ext4 volume at `/mnt/molecule-volume` (or `MOLECULE_STORAGE`; prepare refuses anything else), and run `molecule test` with Docker. `MOLECULE_DISTRO=ubuntu_jammy` tests Ubuntu 22.04. For SRC's Ansible, install `molecule/requirements.txt` with `ansible==14.6.0` instead, `requirements-dev.txt` into a separate virtualenv, and set `MOLECULE_TESTS_PYTHON` to its `python`.

### Manual check on SRC with the Galaxy component

What CI cannot cover: SRC's own volumes, hostnames and certificates, and a real Galaxy.

1. Choose a name and a password (12 or more of `A-Z a-z 0-9 . _ ~ -`). Deploy a Galaxy workspace from [src-component-galaxy-amqp](https://github.com/mirelaminkova/src-component-galaxy-amqp) with `src_galaxy_rabbitmq: true` and `src_galaxy_pulsar_amqp_configs: [{name: <name>, password: <password>, jobs_directory: <pulsar_storage_path>/staging, singularity_enabled: true}]` (the entry this workspace's welcome page shows), and open `src_galaxy_rabbitmq_port` (5671) in its security group for the Pulsar workspace.
2. Deploy this component on a workspace with an attached volume, with `pulsar_galaxy_fqdn` set to the Galaxy workspace's hostname (`pulsar_amqp_host` empty: the broker is that workspace), the same name and password, and `pulsar_storage_path: /data/<volume name>/pulsar`. The deployment succeeds only when Pulsar consumes its setup queue; compare the welcome page's entry with Galaxy's.
3. On the Galaxy workspace: `sudo rabbitmqctl -n rabbit@localhost list_consumers -p /pulsar/<name>` lists `pulsar__setup`, and `sudo rabbitmqctl -n rabbit@localhost list_connections user vhost ssl` shows `<name>  /pulsar/<name>  true`.
4. In Galaxy, choose this Pulsar under User → Preferences → Manage information → **Use distributed compute resources**, upload a small text file and run a tool on it (Sort, say). On the Pulsar workspace, `squeue` shows the job while it runs, `journalctl -u pulsar` its staging, and its files appear under `<pulsar_storage_path>/staging/`. The job turns green in Galaxy and its output is correct.
5. `sudo /opt/pulsar/venv/bin/python /opt/pulsar/check_amqp.py /opt/pulsar/config/app.yml` exits with 0 after a reboot of the Pulsar workspace, too.

## Operations

```bash
systemctl status pulsar
journalctl -u pulsar -f
sudo systemctl restart pulsar
squeue                       # Slurm runner: queued and running jobs
scontrol show job <id>       # one job, up to four hours after it ended
sinfo -N -l                  # the node's state and size
```

## Attached storage

All of Pulsar's data lives on the workspace's attached storage volume, never on the root disk. `pulsar_storage_path` is required. Set to the volume's mount point, `/data/<volume name>`, it stands for the directory `/data/<volume name>/pulsar`, which the deployment creates; any deeper path is used as it is. With `/data/pulsar_storage` (or `/data/pulsar_storage/pulsar`), the layout is:

| Path | Owner | Contents |
| --- | --- | --- |
| `/data/pulsar_storage/pulsar` | `root:root 0755` | the storage directory |
| `…/staging` | `pulsar 0750` | job inputs, outputs and working directories: Pulsar's `staging_directory`, Galaxy's `jobs_directory` |
| `…/home` | `pulsar 0750` | `HOME` of Pulsar and its jobs (the unit's and the task prolog's `HOME`; `/etc/passwd` keeps `/var/lib/pulsar/home`, as `usermod` cannot change the home of a user with running processes) and Pulsar's working directory: what jobs keep under `~`, such as Pulsar's tool `dependencies` and, for Pulsar's own jobs, Apptainer's cache (they keep only `HOME`, `PATH` and `TMPDIR` of the unit's environment) |
| `…/persisted_data` | `pulsar 0750` | job state and message acknowledgement stores |
| `…/apptainer_cache` | `pulsar 0750` | Apptainer's image cache (`APPTAINER_CACHEDIR`) for Slurm jobs, with `pulsar_apptainer`; Pulsar's own jobs (local runner) drop that variable and use `~/.apptainer/cache` in the home |
| `…/apptainer_tmp` | `pulsar 0750` | Apptainer's build space (`APPTAINER_TMPDIR`) for Slurm jobs, with `pulsar_apptainer`; Pulsar's own jobs build in `TMPDIR` |
| `…/job_tmp` | `pulsar 0750` | jobs' temporary files (`TMPDIR`): shared by Pulsar's own jobs (from the `pulsar` unit; tools usually clean up, check it now and then), a directory per Slurm job (from Slurm's task prolog, removed after the job); also Slurm's `TmpFS` |
| `…/slurmd` | `root 0755` | slurmd's spool (`SlurmdSpoolDir`: job scripts and running jobs' state), with `pulsar_job_runner: slurm` |
| `…/docker` | root (created by dockerd) | Docker's `data-root` (images, containers), with `pulsar_docker`; the containerd image store, Docker 29's default, is switched off so that images are here and not in `/var/lib/containerd` |

The venv (`/opt/pulsar/venv`) and the configuration (`/opt/pulsar/config`, with the password) stay on the root disk: they are small, rebuilt by every deployment, and the configuration should not travel with the volume. The storage directory belongs to root, and the playbook does not follow links when it creates directories, so jobs running as `pulsar` cannot make a later deployment hand other directories to `pulsar`.

The check refuses a path on the root file system (also SRC's mount point directory while the volume is not attached), on tmpfs or a similar file system, with a symbolic link in it, that is a volume's mount point itself, or where root cannot create a file and give it to another user (read-only, NFS `root_squash`, FUSE without `allow_other`); with `pulsar_docker`, also one on a network or FUSE file system, where Docker's overlay2 does not work. The storage directory must not be replaceable by other users: below the volume's mount point every existing directory up to the storage directory must be root's, and none may be writable by group or others without the sticky bit. The deployment sets the sticky bit on a volume root that others can write to (they can then still create and use their own files there, but not rename or delete Pulsar's directory).

The `pulsar` unit, and Docker's and slurmd's through drop-ins, run the check (`/usr/local/libexec/pulsar-src/check_storage.py`, as root) before every start: if the volume is missing at boot, however SRC mounts it, they do not start rather than write to the empty mount point on the root disk, and retry every 10 seconds until the volume is there. They deliberately have no `RequiresMountsFor`: a failed or late mount would fail their start as a dependency, which systemd does not retry; `After=local-fs.target remote-fs.target` only stops them before the volume is unmounted at shutdown. Docker's `daemon.json` is written before Docker is installed, so its first start already uses the storage; a Docker installed earlier is restarted onto it (its running containers stop). A drop-in left from an earlier deployment is removed when Docker or the Slurm runner is switched off.

**Reusing a volume.** Rerunning the component, or a new workspace with the same volume and `pulsar_storage_path`, reuses the data on it. If the `pulsar` user's uid or gid differs from the one that wrote it (a new workspace), the playbook stops Pulsar and hands the old owner's files in the `pulsar`-owned directories (`home`, `staging`, `persisted_data`, `job_tmp`, `apptainer_cache` and `apptainer_tmp` above) to the new `pulsar` user, owner and group only; files of other users, and root's, are left alone. Pulsar is started again afterwards, also when a later task fails. The data is kept and reused; whether Pulsar can finish jobs from an earlier workspace depends on that Galaxy still waiting for them. Deleting the workspace keeps the volume.

**Changing the path.** Let running jobs finish first (`squeue` empty: slurmd's spool moves with the path, and Slurm loses jobs that are still running). Set `pulsar_storage_path` to the new directory and rerun this component; it creates a fresh layout there and leaves the old directory as it is (move the data first with Pulsar stopped if you want to keep it). The Galaxy entry's `jobs_directory` changes with it: copy the new entry from the welcome page into `src_galaxy_pulsar_amqp_configs` and rerun the Galaxy component. With Docker, images in the old `data-root` (and in `/var/lib/docker` from a Docker installed before this component) are not moved; Docker pulls them again.

**Upgrading from a version with `pulsar_staging_dir`.** That version kept staging in `pulsar_staging_dir` (default `/mnt/pulsar/files/staging`), persistence in `/var/lib/pulsar/persisted_data`, and the home with Apptainer's cache (`.apptainer`) and Pulsar's tool `dependencies` in `/var/lib/pulsar/home`. Let running jobs finish, set `pulsar_storage_path`, update Galaxy's entry as above and rerun; then remove `/var/lib/pulsar`, the old staging directory and Docker's `/var/lib/docker` to free the root disk (the playbook no longer uses them). To keep job state, cached images or dependencies, stop Pulsar and move `persisted_data`, the staging directory's content, `.apptainer` and `dependencies` into the new layout before the rerun.
