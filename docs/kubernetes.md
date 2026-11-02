# Déploiement Kubernetes

Cluster : Kubernetes de Docker Desktop, **mode kind**, 1 nœud (`desktop-control-plane`),
v1.36.1, arm64. Namespace : `retailpulse`. Toute la plateforme est dans le cluster.

## Procédure

```bash
cp .env.example .env              # mot de passe PostgreSQL (jamais versionné)
scripts/build-images.sh           # retailpulse-app / -spark / -dashboard : 1.0.0
docker compose down               # libère la VM Docker
scripts/k8s-deploy.sh
```

`k8s-deploy.sh` enchaîne les étapes suivantes :
1. **import des images** dans le containerd du nœud kind (`scripts/k8s-load-images.sh`). En mode
   kind, le nœud est un conteneur avec son propre magasin d'images et ne voit pas les images
   construites localement ;
2. Namespace, ConfigMap `retailpulse-config`, Secret `retailpulse-db` créé depuis `.env`
   (`scripts/k8s-secret.sh`), ConfigMap `postgres-init` créée depuis `sql/init.sql` ;
3. StatefulSets `kafka` et `postgres`, puis attente de la disponibilité ;
4. Job `collector`, attente de `Complete`, puis Job `reference-loader`, attente de `Complete` ;
5. Deployments `producer`, `spark`, `dashboard` et `toolbox` ;
6. **accès depuis l'hôte** (`scripts/k8s-expose.sh`) : en mode kind, Docker Desktop ne publie pas
   les NodePorts du nœud vers macOS. Un relais `socat` sur le réseau Docker `kind` publie
   `localhost:30851` et `localhost:30440` vers les NodePorts du nœud, comme le feraient les
   `extraPortMappings` de kind. Le trafic passe donc bien par les Services NodePort.

| Accès | URL |
|---|---|
| Tableau de bord (Service NodePort `dashboard`) | http://localhost:30851 |
| Spark UI (Service NodePort `spark-ui`) | http://localhost:30440 |

Manifestes (`k8s/`, une arborescence par fonction, également applicables via `kustomization.yaml`) :

| Dossier | Ressources |
|---|---|
| `base/` | Namespace, ConfigMap |
| `kafka/` | StatefulSet (PVC 2 Gi, initContainer de permissions), Services headless et ClusterIP |
| `postgres/` | StatefulSet (PVC 2 Gi), Service |
| `jobs/` | Jobs collector et reference-loader |
| `producer/` | Deployment |
| `spark/` | Deployment (`Recreate`), PVC de checkpoints 1 Gi, Service NodePort UI |
| `dashboard/` | Deployment, Service NodePort |
| `tools/` | Deployment `toolbox` (injection d'événements de test) |

## Preuves de déploiement

Journal complet du déploiement : [results/k8s-deploy.log](results/k8s-deploy.log). État après les
tests : [results/k8s-status.log](results/k8s-status.log). Extrait :

```text
NAME                             READY   STATUS      RESTARTS   AGE
pod/collector-d2chv              0/1     Completed   0          9m32s
pod/dashboard-844b8948d9-dtd4x   1/1     Running     0          36s     <- recréé (scénario 8)
pod/kafka-0                      1/1     Running     0          10m
pod/postgres-0                   1/1     Running     0          2m45s   <- recréé (scénario 5)
pod/producer-b87548b6-l89c4      1/1     Running     0          37s     <- recréé (scénario 8)
pod/reference-loader-85xll       0/1     Completed   0          9m27s
pod/spark-75bc8cbb87-k5c6l       1/1     Running     0          4m29s   <- recréé (scénario 4)
pod/toolbox-7649b84465-xjfhv     1/1     Running     0          6m45s

job.batch/collector          Complete   1/1   5s
job.batch/reference-loader   Complete   1/1   7s

service/dashboard        NodePort    10.96.216.141   8501:30851/TCP
service/kafka            ClusterIP   10.96.228.20    29092/TCP
service/kafka-headless   ClusterIP   None            29092/TCP,9093/TCP
service/postgres         ClusterIP   10.96.174.170   5432/TCP
service/spark-ui         NodePort    10.96.201.21    4040:30440/TCP

persistentvolumeclaim/data-kafka-0        Bound   2Gi   RWO   standard
persistentvolumeclaim/data-postgres-0     Bound   2Gi   RWO   standard
persistentvolumeclaim/spark-checkpoints   Bound   1Gi   RWO   standard
```

Contrôle fonctionnel juste après le déploiement : 194 produits, et un CA agrégé de 251 016,84 USD,
identique à la somme des `PAYMENT_COMPLETED` d'`order_events`. `curl http://localhost:30851/_stcore/health` → 200.

## Tests de résilience sur Kubernetes

`MODE=k8s scripts/scenarios/run-all.sh` → [results/scenarios-k8s-20261102-120837.log](results/scenarios-k8s-20261102-120837.log) :
**8 scénarios sur 8 réussis**.

| Scénario | Manipulation Kubernetes | Résultat |
|---|---|---|
| 1. Flux nominal | — | 28 commandes en 30 s, CA agrégé = CA attendu |
| 2. Paiement échoué | injection via `deploy/toolbox` | CA inchangé, statut `PAYMENT_FAILED` |
| 3. Doublon | idem | compté une fois |
| 4. Reprise Spark | `kubectl delete pod -l app=spark` | nouveau pod (stratégie Recreate), **même PVC de checkpoint** : CA cohérent, 0 doublon |
| 5. PostgreSQL indisponible | `kubectl scale statefulset/postgres --replicas=0` pendant 45 s, puis 1 | PVC rattaché, données intactes ; retry du sink, 0 redémarrage de Spark ; CA cohérent |
| 6. Événement tardif | — | 7 min intégré, 30 min ignoré |
| 7. Messages invalides | — | isolés dans `invalid_events` |
| 8. Pod supprimé | `kubectl delete pod` producer puis dashboard | recréés par leur Deployment ; le flux reprend (23 événements en 15 s) |

**Analyse.** Les Deployments et StatefulSets recréent les pods supprimés. Les données survivent
grâce aux PVC (Kafka, PostgreSQL, checkpoints Spark). La perte du pod Spark est sans conséquence
sur les résultats : la reprise par checkpoint et les upserts absorbent le batch rejoué. Kubernetes ne
protège pas de la perte du **nœud** unique, qui porte aussi les volumes `standard` (hostPath). Un
cluster multi-nœuds avec un stockage répliqué serait nécessaire pour cela.

## Pièges rencontrés

| Problème | Cause | Solution |
|---|---|---|
| Kafka risquait de ne pas démarrer avec des propriétés inconnues | Kubernetes injecte `KAFKA_PORT`, `KAFKA_SERVICE_HOST`… (service links), que l'image `apache/kafka` convertit en configuration | `enableServiceLinks: false` sur tous les pods |
| Images introuvables par le nœud | le mode kind a son propre containerd | `docker save | ctr -n k8s.io images import` (`k8s-load-images.sh`) |
| NodePort injoignable depuis macOS | nœud kind = conteneur sans publication de ports | relais `socat` sur le réseau `kind` (`k8s-expose.sh`) |
| Permissions du volume Kafka | volume créé par root, Kafka tourne en uid 1000 | initContainer `chown` + `fsGroup` |
| Spark : deux pods sur un PVC RWO pendant un rollout | stratégie RollingUpdate par défaut | `strategy: Recreate` |
