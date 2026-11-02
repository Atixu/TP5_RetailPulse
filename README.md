# RetailPulse

Plateforme distribuée de traitement de données e-commerce en quasi temps réel
(Master 2 Data — module Données distribuées, Sup de Vinci).

```
DummyJSON ─► Collector ─► Kafka (KRaft) ─► Reference Loader ─► PostgreSQL (référentiels)
                                                                     │
              Producteur (simulation) ◄──────────────────────────────┘
                     │ retail-orders / retail-order-events
                     ▼
         Spark Structured Streaming ─► PostgreSQL (commandes, agrégats) ─► Dashboard Streamlit
```

* Dossier d'architecture, choix et justifications : [docs/architecture.md](docs/architecture.md)
* Tests de résilience : [docs/tests-resilience.md](docs/tests-resilience.md)
* Benchmarks : [docs/benchmarks.md](docs/benchmarks.md)
* Déploiement Kubernetes et preuves : [docs/kubernetes.md](docs/kubernetes.md)
* Support de soutenance : [docs/soutenance.md](docs/soutenance.md)
* Le TP d'introduction de la séance 5 (un événement JSON sur `sales-events`) est conservé dans
  [tp-initial/](tp-initial/).

## Organisation du dépôt

| Chemin | Contenu |
|---|---|
| `apps/` | Image `retailpulse-app` : `collector.py`, `reference_loader.py`, `producer.py`, `inject_event.py`, `events.py` (contrat d'événements), `common/` (config, Kafka, PostgreSQL) |
| `spark/` | Image `retailpulse-spark` : `streaming_job.py`, `transforms.py` (fonctions pures testées), `sink.py` (upserts), `tests/` |
| `dashboard/` | Image `retailpulse-dashboard` : application Streamlit |
| `sql/init.sql` | Schéma PostgreSQL, contraintes et index |
| `docker-compose.yml` | Pile complète en local |
| `k8s/` | Manifestes Kubernetes par fonction (base, kafka, postgres, jobs, producer, spark, dashboard) |
| `scripts/` | Build, déploiement K8s, tests unitaires, scénarios de test (`scenarios/`) |
| `tools/benchmark.py` | Mesures de débit, latence et ressources |
| `tests/` | Tests unitaires du collecteur et du producteur |

## Prérequis

* Docker Desktop (testé avec Docker 29, 8 Go de RAM alloués à la VM) et Docker Compose v2.
* Pour Kubernetes : Docker Desktop → *Settings → Kubernetes → Enable Kubernetes*, et `kubectl`.
* Accès Internet (API DummyJSON et téléchargement des images).

## Configuration

```bash
cp .env.example .env      # puis choisir un mot de passe PostgreSQL
```

`.env` est ignoré par Git : aucun secret n'est versionné. Les autres paramètres ont des valeurs par
défaut documentées dans [docs/architecture.md](docs/architecture.md#rôle-entrées-et-sorties-des-composants),
et peuvent être surchargés par variable d'environnement (`RATE=20 docker compose up -d producer`).

## Démarrage avec Docker Compose

```bash
docker compose up -d --build
docker compose ps                       # collector et reference-loader : « Exited (0) »
docker compose logs collector           # rapport de collecte : 194 produits, 208 clients
```

| Service | Accès |
|---|---|
| Tableau de bord | http://localhost:8501 |
| Spark UI (onglet *Structured Streaming*) | http://localhost:4040 |
| Kafka depuis l'hôte | `localhost:9092` |
| PostgreSQL depuis l'hôte | `localhost:5432` (identifiants de `.env`) |

Commandes utiles :

```bash
# Topics et retard de consommation
docker exec retailpulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
docker exec retailpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic retail-order-events --property print.key=true --max-messages 5

# Contrôles SQL
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse \
  -c "SELECT category, window_start, order_count, total_revenue FROM sales_aggregates ORDER BY window_start DESC LIMIT 10"

# Injecter un événement de test (doublon, retard, invalide…)
docker compose run --rm tools python inject_event.py --type PAYMENT_COMPLETED --duplicate

# Rejouer le chargement des référentiels depuis le début (aucun doublon créé)
REPLAY_FROM_START=true docker compose up reference-loader

# Arrêt (garder les données) / remise à zéro complète
docker compose down
docker compose down -v
```

## Déploiement Kubernetes (Docker Desktop)

```bash
scripts/build-images.sh                 # retailpulse-{app,spark,dashboard}:1.0.0
docker compose down                     # libère les ressources de la VM Docker
scripts/k8s-deploy.sh                   # déploiement ordonné + attente des Jobs
scripts/k8s-status.sh                   # pods, services, PVC, logs
```

* Tableau de bord : http://localhost:30851 ; Spark UI : http://localhost:30440.
* Suppression complète : `scripts/k8s-delete.sh`.

## Tests

```bash
scripts/run-tests.sh                                # 18 tests unitaires (collecteur, producteur, Spark)
scripts/scenarios/run-all.sh                        # 7 scénarios de bout en bout sous Compose
MODE=k8s scripts/scenarios/run-all.sh               # mêmes scénarios + pod supprimé sous Kubernetes
python3 tools/benchmark.py                          # benchmarks (≈ 25 min)
```

Les sorties sont enregistrées dans `docs/results/`.

## Limites connues

* Un seul broker Kafka (RF=1) et une seule instance PostgreSQL : pas de tolérance à la perte d'un nœud.
* Le catalogue d'enrichissement est chargé au démarrage de Spark ; un nouveau produit n'est connu
  qu'après un redémarrage (le catalogue DummyJSON est figé).
* Déduplication Spark bornée par le watermark (10 min) ; les événements plus tardifs sont ignorés
  par les agrégats, tout en restant tracés dans `order_events`.
* L'écriture `foreachBatch` collecte chaque micro-batch sur le driver : c'est simple et
  transactionnel, mais c'est le premier goulot d'étranglement à fort débit (voir les benchmarks).

## Contributions

Travail en binôme : chaque membre commite ses propres changements pour que les contributions soient
identifiables dans l'historique Git.
