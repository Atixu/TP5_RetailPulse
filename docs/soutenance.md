# Soutenance (15 minutes)

Support : deck en ligne de 15 diapositives, avec notes de l'orateur, exportable en PDF et en PPTX
depuis la page : https://claude.ai/artifact/J7TrzaQdsw5Qyhz43TeRZc
(privé par défaut : à partager depuis le menu *Share* avant la soutenance). Remplacer
`[Prénom Nom]` sur la couverture.

## Déroulé proposé

| Temps | Diapositives | Qui |
|---|---|---|
| 0:00 – 2:00 | Couverture, problématique, architecture | membre 1 |
| 2:00 – 6:00 | Collecte, Kafka, contrat d'événements | membre 1 |
| 6:00 – 9:00 | Spark, fiabilité (watermark, dédup, idempotence), PostgreSQL, dashboard | membre 2 |
| 9:00 – 10:30 | Kubernetes | membre 2 |
| 10:30 – 13:00 | Tests de résilience et performances | membre 1 |
| 13:00 – 15:00 | Limites, démonstration de bout en bout | les deux |

## Démonstration de bout en bout (Kubernetes)

```bash
# 1. Ressources réellement déployées
kubectl -n retailpulse get pods,deploy,statefulset,jobs,svc,pvc

# 2. Collecte et référentiels
kubectl -n retailpulse logs job/collector | tail -20
kubectl -n retailpulse exec postgres-0 -- psql -U retailpulse -d retailpulse \
  -c "SELECT (SELECT count(*) FROM products) produits, (SELECT count(*) FROM customers) clients"

# 3. Déclencher une commande et la suivre dans Kafka
kubectl -n retailpulse exec deploy/toolbox -- python inject_event.py --type PAYMENT_COMPLETED --product-id 1 --quantity 2
kubectl -n retailpulse exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic retail-order-events --from-beginning \
  --property print.key=true --max-messages 3

# 4. Vérifier PostgreSQL (détail puis agrégat)
kubectl -n retailpulse exec postgres-0 -- psql -U retailpulse -d retailpulse \
  -c "SELECT order_id, event_type, event_timestamp, ingested_at FROM order_events ORDER BY ingested_at DESC LIMIT 5" \
  -c "SELECT category, window_start, order_count, total_revenue FROM sales_aggregates ORDER BY updated_at DESC LIMIT 5"

# 5. Dashboard : http://localhost:30851 (mis à jour en moins de 10 s)

# 6. Test de résilience en direct (≈ 1 min)
MODE=k8s scripts/scenarios/03-doublon.sh
```

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
