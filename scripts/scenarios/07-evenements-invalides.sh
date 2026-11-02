#!/usr/bin/env bash
# Scénario 7 — Messages invalides ou impossibles à enrichir : isolés dans invalid_events.
source "$(dirname "$0")/lib.sh"
title "Événements invalides"
pause_producer; ensure_injector; wait_stable 6 || true
before="$(total_revenue)"
inject --invalid >/dev/null
inject --type PAYMENT_COMPLETED --unknown-product >/dev/null
wait_stable 6 || true
sql "SELECT reason, count(*) FROM invalid_events GROUP BY reason ORDER BY reason" | sed 's/^/  /'
for reason in JSON_INVALIDE CHAMP_OBLIGATOIRE_MANQUANT PRODUIT_INCONNU; do
  n="$(sql "SELECT count(*) FROM invalid_events WHERE reason = '$reason'")"
  [[ "$n" -gt 0 ]] && pass "$reason isolé ($n)" || fail "$reason absent"
done
assert_eq "CA inchangé" "$before" "$(total_revenue)"
resume_producer
finish
