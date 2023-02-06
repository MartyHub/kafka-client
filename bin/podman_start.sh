#!/usr/bin/env sh

set -eu -o pipefail

CYAN=$'\e[0;36m'
GREEN=$'\e[0;32m'
NC=$'\e[0m'
RED=$'\e[0;31m'
YELLOW=$'\e[0;33m'

if ! type podman >/dev/null; then
  echo "${RED}[ERROR]${NC} podman is required"
  exit 1
fi

if ! type podman-compose >/dev/null; then
  echo "${RED}[ERROR]${NC} podman-compose is required"
  exit 1
fi

container_name=broker
container_id=$(podman ps --filter name="${container_name}" --filter status=running --quiet)

if [[ ! -n "${container_id}" ]]; then
  echo "${CYAN}Starting Kafka...${NC}"
  podman-compose up -d >/dev/null
fi

while ! podman wait --condition=running "${container_name}" >/dev/null 2>&1; do
  echo "${YELLOW}Waiting for Kafka to start...${NC}"
  sleep 1
done

echo "${GREEN}[OK]${NC} Kafka is started"
