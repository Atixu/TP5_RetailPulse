# Tests et résilience

Tous les résultats ci-dessous ont été **réellement mesurés** le 02/11/2026 sous Docker Compose
(Docker Desktop 29, VM de 10 CPU et 8 Go). Les sorties brutes sont dans [results/](results/).

## 1. Tests unitaires — `scripts/run-tests.sh`

| Suite | Image | Tests | Couverture |
|---|---|---|---|
| `tests/test_collector.py` | retailpulse-app | 9 | normalisation produits et clients, rejets, pagination `limit/skip`, collecte incomplète |
| `tests/test_events.py` | retailpulse-app | 4 | format horodatage, prix remisé, unicité des produits et des `event_id`, cohérence des montants, cycle de vie et règle de compensation |
| `spark/tests/test_transforms.py` | retailpulse-spark | 5 | parsing, 8 motifs de rejet, enrichissement catégorie, explosion par catégorie, fenêtres de 5 min |

Résultat : **18 tests réussis** (13 côté applications, 5 côté Spark).

## 2. Scénarios de bout en bout — `scripts/scenarios/`

Chaque script met le producteur en pause quand il a besoin d'un état déterministe, puis compare
les valeurs attendues et obtenues en SQL. Le contrôle de cohérence global
(`check_consistency`) vérifie que :

* **CA agrégé** = `sum(sales_aggregates.total_revenue)` est égal au **CA attendu**, la somme des
  `amount` des `PAYMENT_COMPLETED` présents dans `order_events`. Les seuls événements exclus sont ceux
  antidatés volontairement de plus de 10 min (`ORD-LATE-…`) ;
* aucune ligne d'agrégat n'est dupliquée pour un même (catégorie, fenêtre).

Journal : [results/scenarios-compose-20261102-111851.log](results/scenarios-compose-20261102-111851.log).

| # | Scénario (§13) | Manipulation | Résultat attendu | Résultat obtenu |
|---|---|---|---|---|
| 1 | Flux nominal | production pendant 30 s | messages persistés, agrégats mis à jour | ✅ 32 commandes et 23 agrégats mis à jour en 30 s ; latence moyenne émission → PostgreSQL **3,96 s** ; CA agrégé = CA attendu (365 000,08) |
| 2 | Paiement échoué | `PAYMENT_FAILED` (26,82 USD) puis, pour contrôle, `PAYMENT_COMPLETED` | le FAILED n'augmente pas le CA | ✅ CA inchangé (471 721,94) ; commande au statut `PAYMENT_FAILED` ; le COMPLETED ajoute exactement 26,82 |
| 3 | Doublon | même message (même `event_id`) publié 2 fois | compté une seule fois | ✅ CA +15,92 une seule fois ; une seule ligne `order_events` ; cohérence globale OK |
| 4 | Reprise Spark | `docker kill` de Spark pendant la production, 30 s d'arrêt, relance | reprise depuis le checkpoint sans double comptage | ✅ 74 événements bufferisés dans Kafka traités à la reprise ; CA agrégé = CA attendu (1 052 278,73) ; 0 doublon |
| 5 | PostgreSQL indisponible | arrêt 45 s pendant la production | erreurs, reprise, intégrité | ✅ le sink réessaie avec backoff (2, 4, 8, 16, 30 s), un micro-batch dure 61 s ; **aucun redémarrage** de Spark nécessaire ; CA cohérent (1 361 822,16) |
| 6 | Événement tardif | paiements antidatés de 7 min puis de 30 min | comportement conforme au watermark | ✅ retard de 7 min **intégré** (+68,48) ; retard de 30 min **ignoré** par l'agrégat mais tracé dans `order_events` ; watermark observé = max event time − 10 min |
| 7 | Messages invalides | JSON invalide, message sans `event_id`, produit inconnu | isolés et tracés | ✅ `JSON_INVALIDE`, `CHAMP_OBLIGATOIRE_MANQUANT` et `PRODUIT_INCONNU` dans `invalid_events` ; CA inchangé |
| 8 | Pod indisponible (K8s) | suppression des pods producer et dashboard | recréés par leur Deployment | ✅ nouveaux pods créés, flux repris (23 événements en 15 s) |

Les scénarios 1 à 7 ont été **rejoués sur Kubernetes** avec le même succès : 8 sur 8
([results/scenarios-k8s-20261102-120837.log](results/scenarios-k8s-20261102-120837.log)). Les
manipulations y deviennent `kubectl delete pod` (Spark) et `kubectl scale statefulset/postgres`
(PostgreSQL) ; voir [kubernetes.md](kubernetes.md).

### Rejeu des référentiels

`REPLAY_FROM_START=true docker compose up reference-loader` relit les deux topics de référence
depuis l'offset 0. Le topic contenait **2 328 messages produits et 2 496 messages clients**, car le
collecteur avait été relancé plusieurs fois et la compaction n'avait pas encore nettoyé le segment
actif. Après rejeu, PostgreSQL contient toujours **194 produits et 208 clients** : l'upsert sur
l'identifiant DummyJSON rend le chargement rejouable sans doublon
([results/rejeu-referentiels.log](results/rejeu-referentiels.log)).

## 3. Analyse

* **Paiements échoués** : le filtre `event_type = 'PAYMENT_COMPLETED'` est appliqué dans Spark
  *avant* l'agrégation, et le dashboard ne lit que `sales_aggregates`. Un échec ou une annulation ne
  peut donc pas atteindre le CA.
* **Doublons** : deux barrières se complètent. `dropDuplicatesWithinWatermark(event_id)` protège
  l'agrégat, et la PK `order_events.event_id` protège le détail. Le doublon du scénario 3 a été
  filtré par la première, et la seconde a produit une seule ligne de détail.
* **Reprise Spark** : Spark rejoue depuis le checkpoint le micro-batch interrompu. Le mode update
  renvoie la valeur complète de chaque fenêtre et l'upsert l'écrase, donc le rejeu est idempotent.
  Pendant l'arrêt, Kafka a joué son rôle de tampon (74 événements).
* **PostgreSQL indisponible** : avec 6 tentatives, le backoff couvre environ 60 s de panne sans
  faire échouer la requête. Au-delà, la requête échoue, le conteneur ou le pod redémarre et la
  reprise se fait par checkpoint, comme au scénario 4. Le producteur n'est pas impacté, car Kafka
  découple les deux côtés.
* **Watermark** : il est calculé sur l'**event time**, pas sur l'heure d'arrivée. Conséquence
  observée lors d'un premier passage
  ([results/scenarios-compose-run1-controle-errone.log](results/scenarios-compose-run1-controle-errone.log)) :
  après une suspension d'environ 15 min de la VM Docker, 4 paiements ont été ingérés avec 15 min de
  retard de **traitement**. Ils ont pourtant été comptés, à juste titre, car le watermark n'avait pas
  avancé pendant la pause. Notre premier contrôle SQL excluait à tort les événements ingérés plus de
  10 min après leur horodatage, d'où un écart constant de 94 800,47 sur les scénarios 1, 3, 4 et 5.
  Le contrôle a été corrigé pour n'exclure que les événements antidatés volontairement, puis tous les
  scénarios ont été rejoués sur une pile neuve.

## 4. Garanties de bout en bout

| Risque | Où il est traité | Démontré par |
|---|---|---|
| Doublon dû aux retries du producteur | idempotence Kafka (`enable.idempotence`) | configuration |
| Doublon applicatif (même `event_id`) | Spark (watermark) + PK `order_events` | scénario 3 |
| Rejeu d'un micro-batch | checkpoint + upserts idempotents | scénarios 4 et 5 |
| Rejeu des référentiels | upsert sur l'identifiant DummyJSON | rejeu du loader |
| Message invalide ou non enrichissable | `invalid_events` (motif tracé) | scénario 7 |
| Événement trop tardif | watermark (ignoré dans l'agrégat, tracé dans le détail) | scénario 6 |
