#!/usr/bin/env bash
# Détecte le type de cluster ciblé par le contexte kubectl courant.
# Usage : source scripts/k8s-cluster.sh ; echo "$CLUSTER_KIND $DOCKER_NETWORK"
#   CLUSTER_KIND   = minikube | docker-desktop | other
#   DOCKER_NETWORK = réseau Docker du nœud (minikube ou kind), vide sinon
CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
case "$CONTEXT" in
  minikube)       CLUSTER_KIND=minikube;       DOCKER_NETWORK=minikube ;;
  docker-desktop) CLUSTER_KIND=docker-desktop; DOCKER_NETWORK=kind ;;
  *)              CLUSTER_KIND=other;          DOCKER_NETWORK="" ;;
esac
export CONTEXT CLUSTER_KIND DOCKER_NETWORK
