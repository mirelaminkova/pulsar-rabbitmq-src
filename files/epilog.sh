#!/bin/bash
# Slurm Epilog: Slurm runs it as root on this node after every job, also a
# cancelled one. Cancelling a job kills its processes but not a Docker container
# it started, which runs under dockerd; this stops that container.
#
# After usegalaxy.org's epilog (natefoo), galaxyproject/infrastructure-playbook
# files/slurm/epilog.sh at ef9f31a. Differences: the job's container_config.json
# (written by the job, as the pulsar user) is read with the job user's rights,
# only as a regular file, with time limits; only a plain container name is
# killed. The epilog always succeeds: a failing epilog drains the node.

export PATH=/usr/sbin:/usr/bin:/sbin:/bin
set -o nounset
set -o pipefail

[ -n "${SLURM_JOB_ID:-}" ] || exit 0

(
    workdir=$(timeout 15 scontrol show job "$SLURM_JOB_ID" | sed -n 's/.*\( \|^\)WorkDir=\([^ ]*\)\( \|$\).*/\2/p')
    # Pulsar's DRMAA jobs run in <staging>/<job>/working; Galaxy writes the
    # container's name to <staging>/<job>/configs/container_config.json.
    config="${workdir}/../configs/container_config.json"
    [ -n "$workdir" ] && [ -d "$workdir" ] && [ -f "$config" ] && [ ! -L "$config" ] || exit 0
    name=$(runuser -u "${SLURM_JOB_USER:?}" -- timeout 10 jq -r '.container_name // empty' "$config") || exit 0
    # Never an option or anything but a Docker container name.
    [[ "$name" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] || exit 0
    timeout 30 docker kill "$name"
) >/dev/null 2>&1 || true

exit 0
