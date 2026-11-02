#!/usr/bin/env bash
# Fonctions communes aux scénarios de test.
# MODE=compose (défaut) ou MODE=k8s (namespace retailpulse).
set -euo pipefail

MODE="${MODE:-compose}"
NS="${NS:-retailpulse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then set -a; source .env; set +a; fi
PGUSER_="${POSTGRES_USER:-retailpulse}"
PGDB_="${POSTGRES_DB:-retailpulse}"

title() { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
info()  { printf '  %s\n' "$*"; }
pass()  { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
fail()  { printf '  \033[31mKO\033[0m   %s\n' "$*"; FAILED=1; }
FAILED=0

# Exécute une requête SQL et renvoie le résultat brut (sans en-tête).
sql() {
  if [[ "$MODE" == k8s ]]; then
    kubectl -n "$NS" exec -i postgres-0 -- psql -U "$PGUSER_" -d "$PGDB_" -Atq -c "$1"
  else
    docker exec -i retailpulse-postgres psql -U "$PGUSER_" -d "$PGDB_" -Atq -c "$1"
  fi
}

# Lance inject_event.py avec les arguments fournis ; affiche le JSON de l'événement.
inject() {
  if [[ "$MODE" == k8s ]]; then
    kubectl -n "$NS" exec deploy/toolbox -- python inject_event.py "$@" 2>/dev/null | tail -1
  else
    docker compose run --rm -T tools python inject_event.py "$@" 2>/dev/null | tail -1
  fi
}

json_field() { python3 -c "import json,sys; print(json.loads(sys.stdin.read())['$1'])"; }

pause_producer() {
  info "Pause du producteur (scénario déterministe)"
  if [[ "$MODE" == k8s ]]; then
    kubectl -n "$NS" scale deploy/producer --replicas=0 >/dev/null
    kubectl -n "$NS" wait --for=delete pod -l app=producer --timeout=60s >/dev/null 2>&1 || true
  else
    docker stop retailpulse-producer >/dev/null 2>&1
  fi
}

resume_producer() {
  info "Reprise du producteur"
  if [[ "$MODE" == k8s ]]; then
    kubectl -n "$NS" scale deploy/producer --replicas=1 >/dev/null
  else
    docker start retailpulse-producer >/dev/null 2>&1
  fi
}

# En mode k8s, l'injection passe par le pod toolbox (indépendant du producteur).
ensure_injector() {
  if [[ "$MODE" == k8s ]]; then
    kubectl -n "$NS" rollout status deploy/toolbox --timeout=120s >/dev/null
  fi
}

total_revenue() { sql "SELECT coalesce(sum(total_revenue), 0) FROM sales_aggregates"; }

# CA attendu d'après le journal des événements : tous les paiements réussis, sauf les
# événements antidatés volontairement de plus de 10 min par inject_event.py (ORD-LATE),
# que le watermark doit écarter. NB : un retard de TRAITEMENT (Spark ou VM arrêtés)
# n'exclut rien, car le watermark est calculé sur l'event time, pas sur l'heure d'arrivée.
expected_revenue() {
  sql "SELECT coalesce(sum(amount), 0) FROM order_events
       WHERE event_type = 'PAYMENT_COMPLETED'
         AND NOT (order_id LIKE 'ORD-LATE-%'
                  AND event_timestamp < ingested_at - interval '10 minutes')"
}

# Attend que le pipeline ait tout traité : CA et nombre d'événements stables.
wait_stable() {
  local previous="" current
  for _ in $(seq 1 40); do
    sleep "${1:-6}"
    current="$(sql "SELECT (SELECT count(*) FROM order_events) || '/' ||
                           (SELECT count(*) FROM invalid_events) || '/' ||
                           (SELECT coalesce(sum(total_revenue), 0) FROM sales_aggregates)")"
    [[ "$current" == "$previous" ]] && return 0
    previous="$current"
  done
  return 1
}

assert_eq() {  # assert_eq <libellé> <attendu> <obtenu>
  if python3 -c "import sys; sys.exit(0 if abs(float('$2') - float('$3')) < 0.005 else 1)"; then
    pass "$1 : attendu=$2 obtenu=$3"
  else
    fail "$1 : attendu=$2 obtenu=$3"
  fi
}

check_consistency() {
  local expected actual dups
  expected="$(expected_revenue)"; actual="$(total_revenue)"
  assert_eq "CA agrégé = somme des PAYMENT_COMPLETED du journal" "$expected" "$actual"
  dups="$(sql "SELECT count(*) FROM (SELECT 1 FROM sales_aggregates
              GROUP BY category, window_start, window_end HAVING count(*) > 1) d")"
  assert_eq "Lignes d'agrégat dupliquées" 0 "$dups"
}

finish() {
  if [[ "$FAILED" == 0 ]]; then printf '\n\033[32mScénario réussi\033[0m\n'; else printf '\n\033[31mScénario en échec\033[0m\n'; exit 1; fi
}
