#!/usr/bin/env bash
# Rend les Services NodePort accessibles depuis l'hôte (macOS/Windows) pour un cluster local
# dont le nœud est un conteneur Docker (minikube avec le pilote docker, Docker Desktop en mode kind) :
# ces nœuds ne publient pas leurs NodePorts. Un relais socat, branché sur le réseau Docker du
# nœud, publie localhost:<nodePort> -> <nœud>:<nodePort>. Le trafic passe donc bien par les
# Services NodePort du cluster.
#   scripts/k8s-expose.sh          # dashboard 30851, Spark UI 30440
#   scripts/k8s-expose.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/k8s-cluster.sh
PORTS=(30851 30440)

for port in "${PORTS[@]}"; do
  docker rm -f "retailpulse-nodeport-$port" >/dev/null 2>&1 || true
done
[[ "${1:-}" == stop ]] && exit 0
if [[ -z "$DOCKER_NETWORK" ]]; then
  echo "Contexte « $CONTEXT » : utiliser l'IP d'un nœud (kubectl get nodes -o wide) et le NodePort."
  exit 0
fi

# Nom du nœud = nom de son conteneur : résolu par le DNS du réseau Docker, il reste valable
# après un redémarrage de Docker (contrairement à l'IP du nœud, qui peut changer).
NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
for port in "${PORTS[@]}"; do
  docker run -d --name "retailpulse-nodeport-$port" --restart unless-stopped \
    --network "$DOCKER_NETWORK" -p "127.0.0.1:$port:$port" alpine/socat:1.8.0.1 \
    "TCP-LISTEN:$port,fork,reuseaddr" "TCP:$NODE:$port" >/dev/null
  echo "http://localhost:$port -> NodePort $NODE:$port ($CLUSTER_KIND)"
done
