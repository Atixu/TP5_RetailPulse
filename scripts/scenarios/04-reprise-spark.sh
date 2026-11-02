#!/usr/bin/env bash
# Scénario 4 — Reprise Spark : arrêt puis relance avec le checkpoint, sans double comptage.
source "$(dirname "$0")/lib.sh"
title "Reprise de Spark depuis le checkpoint"
resume_producer
info "Production pendant 20 s, puis arrêt brutal de Spark"
sleep 20
if [[ "$MODE" == k8s ]]; then
  kubectl -n "$NS" delete pod -l app=spark --wait=false >/dev/null
else
  docker kill retailpulse-spark >/dev/null
fi
info "Spark arrêté ; le producteur continue 30 s (Kafka bufferise)"
sleep 30
lag_events="$(sql "SELECT count(*) FROM order_events")"
if [[ "$MODE" == k8s ]]; then
  kubectl -n "$NS" rollout status deploy/spark --timeout=180s >/dev/null
else
  docker start retailpulse-spark >/dev/null 2>&1
fi
info "Spark relancé (order_events avant reprise : $lag_events)"
pause_producer
wait_stable 8 || true
info "order_events après reprise : $(sql "SELECT count(*) FROM order_events")"
check_consistency
resume_producer
finish
