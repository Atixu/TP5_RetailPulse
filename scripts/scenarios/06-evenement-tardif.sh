#!/usr/bin/env bash
# Scénario 6 — Événement tardif : intégré si retard < watermark (10 min), ignoré sinon.
source "$(dirname "$0")/lib.sh"
title "Événements tardifs et watermark (10 minutes)"
resume_producer; sleep 15   # fait avancer le watermark jusqu'à « maintenant − 10 min »
pause_producer; ensure_injector; wait_stable 6 || true
before="$(total_revenue)"
event="$(inject --type PAYMENT_COMPLETED --product-id 10 --quantity 1 --late-minutes 7)"
amount="$(echo "$event" | json_field amount)"
info "Paiement antidaté de 7 min ($(echo "$event" | json_field event_timestamp)), montant $amount"
wait_stable 6 || true
assert_eq "Retard 7 min (< watermark) : intégré" "$(python3 -c "print(round($before + $amount, 2))")" "$(total_revenue)"
before="$(total_revenue)"
event="$(inject --type PAYMENT_COMPLETED --product-id 10 --quantity 1 --late-minutes 30)"
event_id="$(echo "$event" | json_field event_id)"
info "Paiement antidaté de 30 min ($(echo "$event" | json_field event_timestamp))"
wait_stable 6 || true
assert_eq "Retard 30 min (> watermark) : ignoré par l'agrégat" "$before" "$(total_revenue)"
assert_eq "…mais tracé dans order_events" 1 "$(sql "SELECT count(*) FROM order_events WHERE event_id = '$event_id'")"
info "Watermark courant : $(sql "SELECT watermark FROM stream_progress WHERE query_name = 'sales-aggregates' ORDER BY batch_timestamp DESC LIMIT 1")"
resume_producer
finish
