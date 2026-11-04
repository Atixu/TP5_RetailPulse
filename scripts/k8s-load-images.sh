#!/usr/bin/env bash
# Rend les images construites localement visibles par le cluster Kubernetes local.
#   - minikube       : minikube image load
#   - Docker Desktop : le nœud kind a son propre containerd -> import dans le namespace k8s.io
#   - autre cluster  : rien à faire ici (pousser les images vers un registry)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/k8s-cluster.sh
VERSION="${VERSION:-1.0.0}"
IMAGES=(retailpulse-app retailpulse-spark retailpulse-dashboard)

case "$CLUSTER_KIND" in
  minikube)
    for image in "${IMAGES[@]}"; do
      echo "minikube image load $image:$VERSION"
      minikube image load "$image:$VERSION"
    done
    minikube image ls | grep retailpulse ;;
  docker-desktop)
    PLATFORM="linux/$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')"
    for image in "${IMAGES[@]}"; do
      for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
        echo "Import de $image:$VERSION dans le nœud $node ($PLATFORM)"
        docker save --platform "$PLATFORM" "$image:$VERSION" \
          | docker exec -i "$node" ctr -n k8s.io images import --platform "$PLATFORM" - >/dev/null
      done
    done ;;
  *)
    echo "Contexte « $CONTEXT » : poussez les images vers un registry accessible par le cluster." ;;
esac
