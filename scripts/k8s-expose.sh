#!/usr/bin/env bash
# Docker Desktop en mode « kind » : le nœud Kubernetes est un conteneur qui ne publie pas les
# NodePorts vers macOS/Windows. Ce script lance un relais socat sur le réseau Docker « kind »
# qui publie localhost:<nodePort> -> <nœud>:<nodePort> (équivalent des extraPortMappings de kind).
# Le trafic passe donc bien par les Services NodePort du cluster.
#   scripts/k8s-expose.sh          # dashboard 30851, Spark UI 30440
#   scripts/k8s-expose.sh stop
set -euo pipefail
PORTS=(30851 30440)
# Nom du nœud = nom du conteneur kind : résolu par le DNS du réseau Docker « kind », il reste
# valable après un redémarrage de Docker (contrairement à l'IP du nœud, qui peut changer).
NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
for port in "${PORTS[@]}"; do
  docker rm -f "retailpulse-nodeport-$port" >/dev/null 2>&1 || true
  [[ "${1:-}" == stop ]] && continue
  docker run -d --name "retailpulse-nodeport-$port" --restart unless-stopped --network kind \
    -p "127.0.0.1:$port:$port" alpine/socat:1.8.0.1 \
    "TCP-LISTEN:$port,fork,reuseaddr" "TCP:$NODE:$port" >/dev/null
  echo "http://localhost:$port -> NodePort $NODE:$port"
done
