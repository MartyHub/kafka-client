#!/usr/bin/env sh

set -eu

BASEDIR=$(dirname "$0")
. "${BASEDIR}/env.sh"

echo "${CYAN}Stopping Kafka...${NC}"
podman-compose down >/dev/null 2>&1

echo "${GREEN}[OK]${NC} Kafka is stopped"
