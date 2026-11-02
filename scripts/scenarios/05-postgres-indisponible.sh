#!/usr/bin/env bash
# Scénario 5 — PostgreSQL indisponible : interruption puis rétablissement.
source "$(dirname "$0")/lib.sh"
title "PostgreSQL indisponible pendant 45 s"
resume_producer
sleep 10
if [[ "$MODE" == k8s ]]; then
  kubectl -n "$NS" scale statefulset/postgres --replicas=0 >/dev/null
  kubectl -n "$NS" wait --for=delete pod/postgres-0 --timeout=60s >/dev/null 2>&1 || true
else
  docker stop retailpulse-postgres >/dev/null 2>&1
fi
info "PostgreSQL arrêté ; production continue (les événements restent dans Kafka)"
sleep 45
if [[ "$MODE" == k8s ]]; then
  kubectl -n "$NS" scale statefulset/postgres --replicas=1 >/dev/null
  kubectl -n "$NS" rollout status statefulset/postgres --timeout=180s >/dev/null
  kubectl -n "$NS" wait --for=condition=ready pod/postgres-0 --timeout=120s >/dev/null
else
  docker start retailpulse-postgres >/dev/null 2>&1
  until docker exec retailpulse-postgres pg_isready -q; do sleep 2; done
fi
info "PostgreSQL rétabli"
if [[ "$MODE" == k8s ]]; then
  info "Redémarrages Spark : $(kubectl -n "$NS" get pod -l app=spark -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')"
else
  info "Redémarrages Spark : $(docker inspect -f '{{.RestartCount}}' retailpulse-spark), producteur : $(docker inspect -f '{{.RestartCount}}' retailpulse-producer)"
fi
sleep 20
pause_producer
wait_stable 8 || true
check_consistency
resume_producer
finish
