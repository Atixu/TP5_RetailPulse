#!/usr/bin/env bash
# Docker Desktop en mode « kind » : le nœud Kubernetes a son propre containerd et ne voit pas
# les images construites localement. On les importe dans le namespace containerd k8s.io.
# (En mode kubeadm, ou avec un registry, cette étape est inutile.)
set -euo pipefail
VERSION="${VERSION:-1.0.0}"
NODES="$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}')"
PLATFORM="linux/$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')"
for image in retailpulse-app retailpulse-spark retailpulse-dashboard; do
  for node in $NODES; do
    echo "Import de $image:$VERSION dans le nœud $node ($PLATFORM)"
    docker save --platform "$PLATFORM" "$image:$VERSION" \
      | docker exec -i "$node" ctr -n k8s.io images import --platform "$PLATFORM" - >/dev/null
  done
done
docker exec "$(echo "$NODES" | cut -d' ' -f1)" crictl images | grep -E 'IMAGE|retailpulse'
