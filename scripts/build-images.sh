#!/usr/bin/env bash
# Construit les trois images versionnées utilisées par Compose et Kubernetes.
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION="${VERSION:-1.0.0}"
docker build -t "retailpulse-app:$VERSION" apps
docker build -t "retailpulse-spark:$VERSION" spark
docker build -t "retailpulse-dashboard:$VERSION" dashboard
docker images --filter=reference='retailpulse-*' --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}'
