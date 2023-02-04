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

if ! type podman-compose >/dev/null; then
  echo "${RED}[ERROR]${NC} podman-compose is required"
  exit 1
fi

container_id=$(podman ps --filter name=zookeeper --filter status=running --quiet)

if [[ -n "${container_id}" ]]; then
  echo "${CYAN}Stopping Kafka...${NC}"
  podman-compose down >/dev/null 2>&1
fi

echo "${GREEN}[OK]${NC} Kafka is stopped"
