#!/usr/bin/env bash
# Preuves de déploiement : ressources, événements récents et extraits de logs.
set -uo pipefail
NS=retailpulse
kubectl -n $NS get pods,deploy,statefulset,jobs,svc,pvc,configmap,secret -o wide
echo; kubectl -n $NS get events --sort-by=.lastTimestamp | tail -15
for app in collector reference-loader; do echo; echo "--- logs job/$app"; kubectl -n $NS logs job/$app --tail=3; done
for app in producer spark dashboard; do echo; echo "--- logs deploy/$app"; kubectl -n $NS logs deploy/$app --tail=3; done
kubectl top pods -n $NS 2>/dev/null || echo "(kubectl top indisponible : metrics-server non installé)"
