#!/usr/bin/env bash
# Exécute tous les scénarios et conserve la sortie dans docs/results/.
cd "$(dirname "$0")"
mkdir -p ../../docs/results
out="../../docs/results/scenarios-${MODE:-compose}-$(date +%Y%m%d-%H%M%S).log"
for s in 0[1-7]-*.sh; do bash "$s"; done 2>&1 | tee "$out"
[[ "${MODE:-compose}" == k8s ]] && bash 08-pod-supprime.sh 2>&1 | tee -a "$out"
echo "Sortie enregistrée dans $out"
