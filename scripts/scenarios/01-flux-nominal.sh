#!/usr/bin/env bash
# Scénario 1 — Flux nominal : les messages circulent et les agrégats apparaissent.
source "$(dirname "$0")/lib.sh"
title "Flux nominal"
before="$(sql "SELECT (SELECT count(*) FROM orders) || ' commandes, ' || (SELECT count(*) FROM sales_aggregates) || ' agrégats'")"
info "Avant : $before"
info "Attente de 30 s de production…"
sleep 30
after="$(sql "SELECT (SELECT count(*) FROM orders) || ' commandes, ' || (SELECT count(*) FROM sales_aggregates) || ' agrégats'")"
info "Après : $after"
new_orders="$(sql "SELECT count(*) FROM orders WHERE created_at > now() - interval '40 seconds'")"
[[ "$new_orders" -gt 0 ]] && pass "$new_orders nouvelles commandes persistées" || fail "aucune nouvelle commande"
fresh="$(sql "SELECT count(*) FROM sales_aggregates WHERE updated_at > now() - interval '40 seconds'")"
[[ "$fresh" -gt 0 ]] && pass "$fresh agrégats mis à jour récemment" || fail "aucun agrégat récent"
latency="$(sql "SELECT round(avg(extract(epoch FROM ingested_at - event_timestamp))::numeric, 2) FROM order_events WHERE ingested_at > now() - interval '40 seconds'")"
info "Latence moyenne émission -> PostgreSQL (order_events) : ${latency} s"
pause_producer
wait_stable 6 || true
check_consistency
resume_producer
finish
