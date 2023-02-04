#!/usr/bin/env sh

set -eu -o pipefail

CYAN=$'\e[0;36m'
GREEN=$'\e[0;32m'
NC=$'\e[0m'
RED=$'\e[0;31m'

if ! type podman >/dev/null; then
  echo "${RED}[ERROR]${NC} podman is required"
  exit 1
fi

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
