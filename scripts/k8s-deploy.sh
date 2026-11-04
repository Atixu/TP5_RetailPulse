#!/usr/bin/env bash
# Déploiement ordonné de RetailPulse dans le namespace retailpulse.
#   1. namespace, ConfigMap, Secret, schéma SQL
#   2. Kafka et PostgreSQL (StatefulSets) -> attente de la disponibilité
#   3. Job collector -> attente « Complete » ; Job reference-loader -> attente « Complete »
#   4. producer, spark, dashboard (Deployments)
set -euo pipefail
cd "$(dirname "$0")/.."
NS=retailpulse
step() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }

source scripts/k8s-cluster.sh
echo "Cluster ciblé : $CONTEXT ($CLUSTER_KIND)"

step "Chargement des images dans le cluster"
scripts/k8s-load-images.sh

step "Namespace, configuration et secret"
kubectl apply -f k8s/base/namespace.yaml -f k8s/base/configmap.yaml
scripts/k8s-secret.sh
kubectl -n $NS create configmap postgres-init --from-file=init.sql=sql/init.sql \
  --dry-run=client -o yaml | kubectl apply -f -

step "Kafka et PostgreSQL"
kubectl apply -f k8s/kafka/kafka.yaml -f k8s/postgres/postgres.yaml
kubectl -n $NS rollout status statefulset/postgres --timeout=300s
kubectl -n $NS rollout status statefulset/kafka --timeout=300s

step "Job collector (DummyJSON -> Kafka)"
kubectl -n $NS delete job collector --ignore-not-found
kubectl apply -f k8s/jobs/collector-job.yaml
kubectl -n $NS wait --for=condition=complete job/collector --timeout=300s
kubectl -n $NS logs job/collector | tail -5

step "Job reference-loader (Kafka -> PostgreSQL)"
kubectl -n $NS delete job reference-loader --ignore-not-found
kubectl apply -f k8s/jobs/reference-loader-job.yaml
kubectl -n $NS wait --for=condition=complete job/reference-loader --timeout=300s
kubectl -n $NS logs job/reference-loader | tail -2

step "Producteur, Spark et tableau de bord"
kubectl apply -f k8s/producer/producer.yaml -f k8s/spark/spark.yaml -f k8s/dashboard/dashboard.yaml -f k8s/tools/toolbox.yaml
kubectl -n $NS rollout status deploy/producer --timeout=180s
kubectl -n $NS rollout status deploy/spark --timeout=300s
kubectl -n $NS rollout status deploy/dashboard --timeout=180s

step "État du namespace"
kubectl -n $NS get pods,deploy,statefulset,jobs,svc,pvc
step "Accès depuis l'hôte"
scripts/k8s-expose.sh
echo "Tableau de bord : http://localhost:30851   Spark UI : http://localhost:30440"
