#!/usr/bin/env sh

set -eu -o pipefail

BASEDIR=$(dirname "$0")
. "${BASEDIR}/env.sh"

machine=kafka-client

if ! podman machine list | grep -q "${machine}"; then
  echo "${CYAN}Creating podman ${machine} machine...${NC}"
  podman machine init "${machine}"
fi

if ! podman machine list | grep -e "${machine}.*running" -q; then
  echo "${CYAN}Starting podman ${machine} machine...${NC}"
  podman machine start "${machine}"
fi

echo "${GREEN}[OK]${NC} Podman machine ${machine} is running"
