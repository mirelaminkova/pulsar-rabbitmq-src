#!/usr/bin/env bash
# Standalone host installation; does not invoke Ansible or start RabbitMQ.
# https://docs.docker.com/engine/install/ubuntu/
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo 'Run this script with sudo from a normal host terminal.' >&2
    exit 1
fi

. /etc/os-release
if [[ ${ID:-} != ubuntu ]]; then
    echo 'This installer supports Ubuntu only.' >&2
    exit 1
fi
if [[ ! -d /run/systemd/system ]]; then
    echo 'Run this installer on the Ubuntu host with systemd running.' >&2
    exit 1
fi

# Report conflicts rather than remove an existing container runtime.
conflicts=()
for package in docker.io docker-compose docker-compose-v2 docker-doc docker-buildx podman-docker containerd runc; do
    if [[ $(dpkg-query -W -f='${db:Status-Status}' "$package" 2>/dev/null || true) == installed ]]; then
        conflicts+=("$package")
    fi
done
if (( ${#conflicts[@]} )); then
    printf 'Conflicting packages found: %s\n' "${conflicts[*]}" >&2
    echo 'Resolve these packages using the Docker Ubuntu installation documentation, then rerun.' >&2
    exit 1
fi

apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl --fail --silent --show-error --location --connect-timeout 15 --max-time 120 \
    https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod 0644 /etc/apt/keyrings/docker.asc

cat > /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker
docker version
docker compose version
docker run --rm hello-world
