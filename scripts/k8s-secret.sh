#!/usr/bin/env bash
# Crée (ou met à jour) le Secret PostgreSQL à partir de .env : aucun secret n'est versionné.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo "Créer .env à partir de .env.example" >&2; exit 1; }
set -a; source .env; set +a
kubectl get namespace retailpulse >/dev/null 2>&1 || kubectl apply -f k8s/base/namespace.yaml
kubectl -n retailpulse create secret generic retailpulse-db \
  --from-literal=POSTGRES_DB="$POSTGRES_DB" \
  --from-literal=POSTGRES_USER="$POSTGRES_USER" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -
