# Mesure des performances

Mesures réalisées le 02/11/2026 avec `tools/benchmark.py`, sous Docker Compose : Docker Desktop 29
sur Mac Apple Silicon, VM de 10 CPU et 8 Go. Données brutes :
[results/benchmark-20261102-114431.json](results/benchmark-20261102-114431.json) (expériences 1 à 3) et
[results/benchmark-20261102-120012.json](results/benchmark-20261102-120012.json) (saturation).

## 1. Méthode

Chaque configuration est un **run indépendant et reproductible**. Le script crée des topics dédiés
(`bench-orders-<id>`, `bench-events-<id>`) avec le nombre de partitions voulu, puis démarre Spark
avec un checkpoint neuf. Le producteur est ensuite lancé en mode `RATE` (cadence régulière). Après
25 s de chauffe, la mesure dure 90 s, puis le producteur est arrêté proprement (SIGTERM).

| Indicateur | Méthode de mesure |
|---|---|
| Débit produit | bilan JSON du producteur (`events_per_s` = événements acquittés / durée) |
| Débit traité | lignes `order_events` ingérées pendant la fenêtre de mesure / 90 s |
| Latence → PostgreSQL | `order_events.ingested_at − event_timestamp` (horloge commune : la VM Docker) ; p50 et p95 |
| Latence → agrégat | `aggregate_writes.written_at − max_event_time` : délai entre le **dernier événement pris en compte** dans une fenêtre et l'instant où l'agrégat qui l'inclut devient lisible par le dashboard |
| Durée des micro-batches | `stream_progress` (StreamingQueryListener Spark) |
| Drain | temps entre l'arrêt du producteur et l'arrivée en base du dernier événement |
| CPU / RAM | `docker stats` échantillonné toutes les ~3 s (moyenne et maximum) |

Une commande génère en moyenne **2,1 événements** : ORDER_CREATED, un résultat de paiement et,
dans 10 % des cas, une annulation. `RATE=100` commandes/s produit donc ≈ 210 événements/s.
Le trigger Spark est de **5 s** dans toutes les expériences.

## 2. Résultats

### Expérience 1 — Débit de production (3 partitions, `local[2]`)

| Commandes/s | Produit (évt/s) | Traité (évt/s) | Latence → PG p50 / p95 | Latence → agrégat p50 / p95 | Batch agrégats moy. / max | Drain |
|---|---|---|---|---|---|---|
| 2 | 4,2 | 4,2 | 4,09 / 6,57 s | 4,64 / 7,14 s | 2 342 / 2 747 ms | 4,3 s |
| 20 | 41,8 | 41,8 | 3,14 / 5,39 s | 1,45 / 3,49 s | 939 / 1 581 ms | 4,3 s |
| 100 | 210,2 | 210,2 | 2,80 / 5,04 s | 0,48 / 0,96 s | 380 / 447 ms | 6,4 s |

### Expérience 2 — Partitions Kafka (100 commandes/s, `local[2]`)

| Partitions | Traité (évt/s) | Latence → PG p50 / p95 | Latence → agrégat p50 / p95 | Batch orders-detail moy. | Batch agrégats moy. / max |
|---|---|---|---|---|---|
| 1 | 210,3 | 2,69 / 4,94 s | 0,38 / 0,83 s | 215 ms | 274 / 306 ms |
| 3 | 210,2 | 2,80 / 5,04 s | 0,48 / 0,96 s | 319 ms | 380 / 447 ms |
| 6 | 209,9 | 3,10 / 5,34 s | 0,89 / 1,42 s | 616 ms | 796 / 1 184 ms |

### Expérience 3 — Parallélisme Spark (100 commandes/s, 6 partitions)

| Spark | Traité (évt/s) | Latence → PG p50 / p95 | Latence → agrégat p50 / p95 | Batch orders-detail moy. | Batch agrégats moy. / max | Drain |
|---|---|---|---|---|---|---|
| `local[1]` | 209,3 | 3,58 / 5,84 s | 1,60 / 2,42 s | 1 110 ms | 1 506 / 2 385 ms | 8,6 s |
| `local[2]` | 209,9 | 3,10 / 5,34 s | 0,89 / 1,42 s | 616 ms | 796 / 1 184 ms | 6,8 s |
| `local[4]` | 210,5 | 2,92 / 5,16 s | 0,59 / 1,00 s | 444 ms | 492 / 642 ms | 4,3 s |

### Expérience 4 — Recherche de la saturation (3 partitions, `local[2]`)

| Commandes/s | Produit (évt/s) | Traité (évt/s) | Latence → PG p50 / p95 | Lignes / batch orders-detail | Batch orders-detail moy. / max | Batch agrégats moy. | RAM Spark max |
|---|---|---|---|---|---|---|---|
| 100 | 210 | 210 | 2,80 / 5,04 s | ≈ 1 050 | 319 / — ms | 380 ms | 815 Mio |
| 500 | 1 052 | 1 052 | 3,15 / 5,37 s | 5 258 | 695 / 749 ms | 394 ms | 990 Mio |
| 1 500 | 3 151 | 3 151 | 4,29 / 6,43 s | 15 755 | 1 920 / 2 537 ms | 653 ms | 1 205 Mio |

### Ressources

* **Spark** : 700 Mio à 1,2 Go de RAM selon le volume (driver JVM et état des fenêtres). Le CPU
  fonctionne par rafales à chaque trigger, jusqu'à 220 % avec `local[4]`. Les moyennes `docker stats`
  sont peu significatives : l'échantillonnage toutes les 3 s tombe souvent entre deux batches.
* **Kafka** : 6 à 21 % de CPU en moyenne et 1,1 à 2,6 Go de RAM, cache de pages inclus. Il n'est
  jamais limitant.
* **PostgreSQL** : moins de 3 % de CPU et environ 150 Mio. **Producteur Python** : 5 % de CPU à
  210 évt/s et 18 % à 3 150 évt/s.

## 3. Analyse

1. **Aucune saturation jusqu'à 3 150 événements/s.** Dans tous les runs, le débit traité égale le
   débit produit et le drain reste sous 10 s. Le pipeline suit donc la charge.
2. **La latence de bout en bout est dominée par le trigger de 5 s**, pas par le calcul. Un événement
   attend en moyenne la moitié de l'intervalle (2,5 s), puis la durée du batch. C'est ce qu'on observe :
   p50 de 2,7 à 3,6 s, et p95 d'environ 5 à 6 s, soit l'intervalle complet plus le batch.
3. **Latence vers l'agrégat.** Elle diminue quand le débit augmente (4,6 s → 0,5 s). Ce n'est pas
   un paradoxe : elle mesure le délai du *dernier* événement inclus dans une écriture. À fort débit,
   ce dernier événement arrive juste avant le trigger. À 2 commandes/s, il peut précéder le trigger de
   plusieurs secondes, et les batches sont plus lents (2,3 s) : JVM peu sollicitée, coût fixe de
   planification et état peu chaud.
4. **Plus de partitions n'est pas toujours mieux.** À débit modéré, passer de 1 à 6 partitions
   *double* la durée des batches (274 → 796 ms). Chaque partition devient une tâche Spark : avec
   2 threads, 6 tâches minuscules coûtent plus en planification qu'elles ne rapportent en
   parallélisme. Les partitions servent à monter en charge, car elles bornent le parallélisme
   maximal de lecture, et ne se justifient qu'avec assez de cœurs et de volume.
5. **Le parallélisme Spark paie quand il y a des tâches à répartir.** Avec 6 partitions, la durée des
   batches baisse presque linéairement : 1 506 ms avec `local[1]`, 796 ms avec `local[2]`, 492 ms
   avec `local[4]`. La latence p95 vers l'agrégat passe de 2,4 à 1,0 s, et le drain de 8,6 à 4,3 s.
6. **Goulot d'étranglement identifié : la requête `orders-detail`.** Sa durée de batch croît
   linéairement avec le volume (≈ 8 000 lignes/s) : 0,3 s pour 1 000 lignes, 1,9 s pour 15 800 lignes.
   Le sink **collecte le micro-batch sur le driver** et l'écrit ligne à ligne avec `executemany`.
   Par extrapolation, la saturation interviendrait vers **8 000 événements/s**, lorsque la durée d'un
   batch atteindrait le trigger de 5 s. La requête d'agrégation, elle, ne renvoie qu'une ligne par
   (catégorie, fenêtre) modifiée et reste rapide (0,65 s à 3 150 évt/s).

## 4. Améliorations proposées

| Levier | Effet attendu |
|---|---|
| Trigger à 1–2 s | latence p50 divisée par 2 à 3, au prix de batches plus nombreux |
| Sink `orders-detail` en `COPY` vers une table de staging puis `INSERT … SELECT … ON CONFLICT`, ou écriture JDBC distribuée via `foreachPartition` | supprime le goulot de collecte sur le driver ; débit multiplié |
| `maxOffsetsPerTrigger` | borne la taille des batches lors d'un rattrapage (après une panne) |
| Partitions = multiple du nombre de cœurs Spark ; `spark.sql.shuffle.partitions` aligné | évite les tâches minuscules (expérience 2) |
| Spark en cluster (master et workers, ou Spark-on-Kubernetes) | parallélisme au-delà d'un nœud |
| Kafka à 3 brokers, RF=3 | tolérance aux pannes (le débit n'est pas en cause ici) |

## 5. Reproduire

```bash
docker compose up -d --build
python3 tools/benchmark.py                          # expériences 1 à 3 (≈ 25 min)
python3 tools/benchmark.py --experiments saturation # expérience 4 (≈ 7 min)
```

> Note : pendant les derniers runs de l'expérience 3, le cluster Kubernetes de Docker Desktop
> démarrait sur la même VM. Cela peut expliquer une partie de la variance (CPU partagé). Les
> tendances restent nettes et monotones.
