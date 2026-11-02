#!/usr/bin/env bash
# Scénario 2 — Paiement échoué : PAYMENT_FAILED n'augmente pas le chiffre d'affaires.
source "$(dirname "$0")/lib.sh"
title "Paiement échoué"
pause_producer; ensure_injector; wait_stable 6 || true
before="$(total_revenue)"
event="$(inject --type PAYMENT_FAILED --product-id 1 --quantity 3)"
order_id="$(echo "$event" | json_field order_id)"
info "PAYMENT_FAILED injecté pour $order_id (montant $(echo "$event" | json_field amount))"
wait_stable 6 || true
assert_eq "CA total inchangé" "$before" "$(total_revenue)"
status="$(sql "SELECT status FROM orders WHERE order_id = '$order_id'")"
[[ "$status" == PAYMENT_FAILED ]] && pass "Statut de la commande : $status" || fail "Statut inattendu : '$status'"
event="$(inject --type PAYMENT_COMPLETED --product-id 1 --quantity 3)"
amount="$(echo "$event" | json_field amount)"
wait_stable 6 || true
assert_eq "Contrôle : un PAYMENT_COMPLETED ajoute son montant" \
  "$(python3 -c "print(round($before + $amount, 2))")" "$(total_revenue)"
resume_producer
finish
