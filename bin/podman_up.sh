#!/usr/bin/env sh

set -eu

BASEDIR=$(dirname "$0")
. "${BASEDIR}/env.sh"

echo "${CYAN}Starting Kafka...${NC}"
podman-compose up -d >/dev/null 2>&1

while ! podman wait --condition=running broker1 >/dev/null 2>&1; do
  echo "${YELLOW}Waiting for Kafka to start...${NC}"
  sleep 1
done

while ! podman wait --condition=running broker2 >/dev/null 2>&1; do
  echo "${YELLOW}Waiting for Kafka to start...${NC}"
  sleep 1
done

echo "${GREEN}[OK]${NC} Kafka is started"
