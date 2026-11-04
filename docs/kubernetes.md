# Déploiement Kubernetes

Toute la plateforme est déployée dans le namespace `retailpulse`. Les **mêmes manifestes** ont été
déployés et testés sur deux clusters locaux à un nœud :

| Cluster | Version | Statut |
|---|---|---|
| **minikube** (pilote docker, 4 CPU, 6 Go) — cible principale | minikube v1.39.0, Kubernetes v1.37.0 | déployé, 8 scénarios de résilience rejoués |
| Docker Desktop Kubernetes (mode kind) | Kubernetes v1.36.1 | déployé, 8 scénarios de résilience |

Les scripts détectent le cluster visé par le contexte `kubectl` courant (`scripts/k8s-cluster.sh`)
et adaptent les deux seules étapes qui diffèrent : le chargement des images et l'accès depuis l'hôte.

## Procédure avec minikube

```bash
brew install minikube                                   # une seule fois
minikube start --driver=docker --cpus=4 --memory=6144   # crée le cluster et bascule kubectl dessus
cp .env.example .env                                    # mot de passe PostgreSQL (jamais versionné)
scripts/build-images.sh                                 # retailpulse-app / -spark / -dashboard : 1.0.0
docker compose down                                     # libère la mémoire de Docker
scripts/k8s-deploy.sh                                   # ≈ 3 min
```

Arrêter sans rien perdre : `minikube stop`. Reprendre : `minikube start`, puis `scripts/k8s-expose.sh`.
Tout supprimer : `scripts/k8s-delete.sh` (le namespace) ou `minikube delete` (le cluster).

Avec Docker Desktop : activer Kubernetes dans les réglages, `kubectl config use-context docker-desktop`,
puis `scripts/build-images.sh && scripts/k8s-deploy.sh`.

Les deux clusters ne doivent pas faire tourner RetailPulse en même temps : la VM Docker a 8 Go.

## Ce que fait `k8s-deploy.sh`

1. **chargement des images** (`scripts/k8s-load-images.sh`) : `minikube image load` sur minikube,
   import dans le containerd du nœud sur Docker Desktop (kind). Dans les deux cas, le nœud est un
   conteneur qui a son propre magasin d'images et ne voit pas les images construites localement ;
2. Namespace, ConfigMap `retailpulse-config`, Secret `retailpulse-db` créé depuis `.env`
   (`scripts/k8s-secret.sh`), ConfigMap `postgres-init` créée depuis `sql/init.sql` ;
3. StatefulSets `kafka` et `postgres`, puis attente de la disponibilité ;
4. Job `collector`, attente de `Complete`, puis Job `reference-loader`, attente de `Complete` ;
5. Deployments `producer`, `spark`, `dashboard` et `toolbox` ;
6. **accès depuis l'hôte** (`scripts/k8s-expose.sh`) : sur macOS, ni minikube (pilote docker) ni
   Docker Desktop (kind) ne publient les NodePorts du nœud. Un relais `socat`, branché sur le réseau
   Docker du nœud (`minikube` ou `kind`), publie `localhost:30851` et `localhost:30440` vers les
   NodePorts. Le trafic passe donc bien par les Services NodePort. Équivalent sans relais sur
   minikube : `minikube service dashboard -n retailpulse`.

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

## Résultats sur minikube (cible principale)

Déploiement par `scripts/k8s-deploy.sh` : [results/k8s-deploy-minikube.log](results/k8s-deploy-minikube.log),
réussi du premier essai, en environ 2 minutes après le chargement des images. Il a donné 2 Jobs
`Complete`, 6 pods `Running` et 3 PVC `Bound` (classe `standard` de minikube). État après les
tests : [results/k8s-status-minikube.log](results/k8s-status-minikube.log).

`MODE=k8s scripts/scenarios/run-all.sh` →
[results/scenarios-minikube-20261104-100648.log](results/scenarios-minikube-20261104-100648.log) :
**8 scénarios sur 8 réussis**, avec les mêmes manipulations que ci-dessous. Faits marquants :
- latence moyenne émission → PostgreSQL de 4,1 s ;
- Spark recréé avec son PVC de checkpoint sans double comptage ;
- PostgreSQL coupé 45 s, puis repris sans redémarrage de Spark ;
- producer et dashboard recréés, le flux reprenant avec 19 événements en 15 s.

## Tests de résilience sur Kubernetes (Docker Desktop)

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
