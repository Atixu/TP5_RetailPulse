#!/usr/bin/env bash
# Scénario 3 — Doublon : le même event_id publié deux fois n'est compté qu'une fois.
source "$(dirname "$0")/lib.sh"
title "Doublon (même event_id publié deux fois)"
pause_producer; ensure_injector; wait_stable 6 || true
before="$(total_revenue)"
event="$(inject --type PAYMENT_COMPLETED --product-id 5 --quantity 2 --duplicate)"
event_id="$(echo "$event" | json_field event_id)"
amount="$(echo "$event" | json_field amount)"
info "event_id=$event_id publié $(echo "$event" | json_field copies) fois, montant=$amount"
wait_stable 6 || true
assert_eq "CA augmenté une seule fois" "$(python3 -c "print(round($before + $amount, 2))")" "$(total_revenue)"
assert_eq "Lignes order_events pour cet event_id" 1 "$(sql "SELECT count(*) FROM order_events WHERE event_id = '$event_id'")"
check_consistency
resume_producer
finish
