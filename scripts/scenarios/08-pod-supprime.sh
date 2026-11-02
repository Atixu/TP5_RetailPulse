#!/usr/bin/env bash
# Scénario 8 (Kubernetes) — Pod applicatif supprimé : recréé par son contrôleur.
MODE=k8s
source "$(dirname "$0")/lib.sh"
title "Suppression de pods applicatifs (producer, dashboard)"
for app in producer dashboard; do
  old="$(kubectl -n "$NS" get pod -l app=$app -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NS" delete pod "$old" --wait=false >/dev/null
  info "Pod $old supprimé"
  kubectl -n "$NS" rollout status deploy/$app --timeout=180s >/dev/null
  new="$(kubectl -n "$NS" get pod -l app=$app --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$new" && "$new" != "$old" ]] && pass "Deployment $app : nouveau pod $new" || fail "$app non recréé"
done
sleep 15
fresh="$(sql "SELECT count(*) FROM order_events WHERE ingested_at > now() - interval '15 seconds'")"
[[ "$fresh" -gt 0 ]] && pass "Le flux a repris ($fresh événements en 15 s)" || fail "flux arrêté"
finish
