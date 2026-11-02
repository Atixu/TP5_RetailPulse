#!/usr/bin/env bash
cd "$(dirname "$0")/.."
# Supprime le namespace (et donc les PVC : données Kafka, PostgreSQL et checkpoints).
scripts/k8s-expose.sh stop 2>/dev/null || true
kubectl delete namespace retailpulse
