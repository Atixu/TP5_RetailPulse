# Soutenance (15 minutes)

Support : deck en ligne de 15 diapositives, avec notes de l'orateur, exportable en PDF et en PPTX
depuis la page : https://claude.ai/artifact/J7TrzaQdsw5Qyhz43TeRZc
(privé par défaut : à partager depuis le menu *Share* avant la soutenance).

## Déroulé (15 minutes)

Les notes de l'orateur de chaque diapositive contiennent le **texte complet à dire**, précédé de
son créneau et de l'orateur. Le script fait environ 2 000 mots, soit 14 min 25 s à un débit oral
moyen de 140 mots par minute, plus 30 s d'actions pour la démo. Total : 14 min 55 s.

| Créneau | Diapositive | Qui |
|---|---|---|
| 0:00 – 0:30 | Couverture | William |
| 0:30 – 1:30 | Problématique | William |
| 1:30 – 2:40 | Architecture | William |
| 2:40 – 3:45 | Collecte et référentiels | William |
| 3:45 – 4:45 | Kafka | William |
| 4:45 – 5:55 | Contrat des événements | William |
| 5:55 – 7:10 | Spark Structured Streaming | Guillaume |
| 7:10 – 8:20 | Fiabilité (watermark, doublons, idempotence) | Guillaume |
| 8:20 – 9:00 | PostgreSQL | Guillaume |
| 9:00 – 9:35 | Tableau de bord | Guillaume |
| 9:35 – 10:40 | Kubernetes | Guillaume |
| 10:40 – 11:50 | Tests de résilience | William |
| 11:50 – 13:00 | Performances | Guillaume |
| 13:00 – 13:40 | Limites | Guillaume |
| 13:40 – 14:55 | Conclusion et démo | les deux |

Temps de parole : William ≈ 7 min 10, Guillaume ≈ 6 min 35, conclusion commune ≈ 1 min 15.
Si vous parlez plus lentement (130 mots/min), le script dure environ 15 min 30 : raccourcissez alors la
démo à l'injection et à l'affichage sur le dashboard.

## Démonstration de bout en bout (Kubernetes)

### Avant la soutenance (5 minutes avant)

1. Démarrer Docker Desktop et attendre que Kubernetes soit *running* (icône verte).
2. Ouvrir un terminal **dans le dossier du projet** :
   ```bash
   cd chemin/vers/TP5_RetailPulse
   kubectl -n retailpulse get pods        # tous en Running
   scripts/k8s-expose.sh                  # rend le dashboard accessible (à relancer après un redémarrage)
   ```
   Si le namespace n'existe pas (cluster réinitialisé) : `scripts/k8s-deploy.sh` (≈ 3 min).
3. Ouvrir http://localhost:30851 dans le navigateur.
4. Ouvrir un **second terminal**, également dans le dossier du projet, pour Kafka (étape 3).

Toutes les commandes ci-dessous ont été exécutées et vérifiées depuis le dossier du projet.

### Pendant la démo

```bash
# 1. Ressources réellement déployées
kubectl -n retailpulse get pods,deploy,statefulset,jobs,svc,pvc

# 2. Collecte et référentiels
kubectl -n retailpulse logs job/collector | tail -20
kubectl -n retailpulse exec postgres-0 -- psql -U retailpulse -d retailpulse \
  -c "SELECT (SELECT count(*) FROM products) produits, (SELECT count(*) FROM customers) clients"
```

```bash
# 3a. TERMINAL 2 : écouter Kafka en direct (laisser tourner ; Ctrl+C pour arrêter)
kubectl -n retailpulse exec -it kafka-0 -c kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic retail-order-events --property print.key=true
```

```bash
# 3b. TERMINAL 1 : injecter un paiement réussi ; noter son order_id (ORD-TEST-…)
#     Il apparaît aussitôt dans le terminal 2, au milieu des commandes du producteur.
kubectl -n retailpulse exec deploy/toolbox -- python inject_event.py --type PAYMENT_COMPLETED --product-id 1 --quantity 2

# 4. Vérifier PostgreSQL quelques secondes plus tard (détail, puis agrégat de la catégorie « beauty »)
kubectl -n retailpulse exec postgres-0 -- psql -U retailpulse -d retailpulse \
  -c "SELECT order_id, event_type, event_timestamp, ingested_at FROM order_events WHERE order_id LIKE 'ORD-TEST-%' ORDER BY ingested_at DESC LIMIT 3" \
  -c "SELECT category, window_start, order_count, total_revenue, updated_at FROM sales_aggregates WHERE category = 'beauty' ORDER BY window_start DESC LIMIT 2"

# 5. Dashboard http://localhost:30851 : la commande ORD-TEST-… apparaît dans « Dernières commandes »
#    au prochain rafraîchissement (≤ 10 s).

# 6. (si le temps le permet, ≈ 1 min) test de résilience en direct : doublon compté une seule fois
MODE=k8s scripts/scenarios/03-doublon.sh
```

### En cas de problème

| Symptôme | Solution |
|---|---|
| Le dashboard ne s'affiche pas | `scripts/k8s-expose.sh` |
| `kubectl` : *connection refused* | Docker Desktop ou Kubernetes pas encore démarré : attendre |
| Pods en `CrashLoopBackOff` juste après le démarrage | attendre 1 à 2 min (Spark attend Kafka et PostgreSQL) |
| `jobs.batch "collector" not found` | `kubectl apply -f k8s/jobs/collector-job.yaml` (rejouable sans doublon) |

## Questions probables

* **Pourquoi `order_id` comme clé ?** Pour l'ordre du cycle de vie d'une commande dans une
  partition, et une forte cardinalité qui évite les hot partitions.
* **Que se passe-t-il si Spark tombe ?** Kafka conserve les événements. Au redémarrage, le
  checkpoint fournit les offsets et l'état, le micro-batch interrompu est rejoué et les upserts le
  rendent sans effet (scénario 4).
* **Un doublon 1 heure plus tard ?** Hors du watermark : Spark ne le déduplique plus dans
  l'agrégat. La PK `order_events` le bloque côté détail. C'est une limite documentée.
* **Pourquoi pas exactly-once de bout en bout ?** Le sink PostgreSQL n'est pas transactionnel avec
  Kafka ; on obtient l'*effectively-once* par idempotence du sink, l'approche recommandée en séance 5.
* **Pourquoi 3 partitions si 1 est plus rapide dans le benchmark ?** À faible débit, les petites
  tâches coûtent plus qu'elles ne rapportent. Les partitions fixent le parallélisme *maximal* futur ;
  3 est un compromis pour 2 cœurs Spark.
