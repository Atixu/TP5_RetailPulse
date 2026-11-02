#!/usr/bin/env python3
"""Benchmarks RetailPulse sous Docker Compose (bibliothèque standard uniquement).

Pour chaque configuration (débit, partitions Kafka, cœurs Spark), le script :
  1. relance producer et spark avec des topics et un checkpoint dédiés
     (bench-orders-p<N>, /checkpoints/bench-<id>) : runs indépendants et reproductibles ;
  2. laisse chauffer WARMUP secondes, puis mesure pendant DURATION secondes ;
  3. arrête le producteur (SIGTERM) et mesure le temps de vidage du retard (drain) ;
  4. calcule, à partir de PostgreSQL :
     - débit produit (journal du producteur) et débit traité (order_events.ingested_at) ;
     - latence émission -> order_events (p50/p95/max) ;
     - latence émission -> agrégat visible (sales_aggregates.updated_at − max_event_time) ;
     - durée des micro-batches Spark (stream_progress) ;
     - CPU / RAM des conteneurs (docker stats échantillonné).

Usage : python3 tools/benchmark.py [--experiments rate,partitions,cores] [--duration 90]
Résultats : docs/results/benchmark-<horodatage>.json et .md
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"

EXPERIMENTS = {
    # Expérience 1 : montée en débit (commandes/s ; ≈ 2,1 événements par commande).
    "rate": [dict(rate=r, partitions=3, master="local[2]") for r in (2, 20, 100)],
    # Expérience 2 : nombre de partitions Kafka à débit élevé.
    "partitions": [dict(rate=100, partitions=p, master="local[2]") for p in (1, 3, 6)],
    # Expérience 3 : parallélisme Spark (threads d'exécution du mode local).
    "cores": [dict(rate=100, partitions=6, master=m) for m in ("local[1]", "local[2]", "local[4]")],
    # Expérience 4 : recherche du point de saturation.
    "saturation": [dict(rate=r, partitions=3, master="local[2]") for r in (500, 1500)],
}
CONTAINERS = ["retailpulse-spark", "retailpulse-kafka", "retailpulse-postgres", "retailpulse-producer"]


def load_env():
    env = dict(os.environ)
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                env.setdefault(key.strip(), value.strip())
    return env


ENV = load_env()


def sh(*args, env=None, check=True):
    return subprocess.run(args, cwd=ROOT, env=env or ENV, check=check,
                          capture_output=True, text=True).stdout


def sql(query):
    out = sh("docker", "exec", "-i", "retailpulse-postgres", "psql",
             "-U", ENV.get("POSTGRES_USER", "retailpulse"),
             "-d", ENV.get("POSTGRES_DB", "retailpulse"), "-AtqF", "|", "-c", query)
    return [line.split("|") for line in out.strip().splitlines() if line]


def scalar(query):
    rows = sql(query)
    value = rows[0][0] if rows and rows[0] else ""
    return float(value) if value not in ("", None) else None


def parse_size(text):
    match = re.match(r"([\d.]+)\s*([KMG]i?B|B)", text.strip())
    if not match:
        return 0.0
    factor = {"B": 1 / 2**20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1,
              "GiB": 1024, "GB": 1024}[match.group(2)]
    return float(match.group(1)) * factor


class StatsSampler(threading.Thread):
    """Échantillonne docker stats toutes les ~5 s pendant la mesure."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples = {name: [] for name in CONTAINERS}
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            out = sh("docker", "stats", "--no-stream", "--format",
                     "{{.Name}};{{.CPUPerc}};{{.MemUsage}}", *CONTAINERS, check=False)
            for line in out.splitlines():
                name, cpu, mem = line.split(";")
                self.samples[name].append((float(cpu.rstrip("%") or 0), parse_size(mem.split("/")[0])))
            self.stop_event.wait(3)

    def summary(self):
        result = {}
        for name, values in self.samples.items():
            if values:
                cpus, mems = zip(*values)
                result[name.replace("retailpulse-", "")] = {
                    "cpu_avg_pct": round(statistics.mean(cpus), 1), "cpu_max_pct": round(max(cpus), 1),
                    "mem_avg_mib": round(statistics.mean(mems)), "mem_max_mib": round(max(mems))}
        return result


def percentiles(query):
    row = sql(query)[0]
    return {k: (round(float(v), 2) if v else None) for k, v in zip(("p50", "p95", "max", "n"), row)}


def aggregate_latency(t0, t1):
    """Délai entre le dernier événement pris en compte et sa visibilité dans l'agrégat.

    Calculé sur le journal append-only aggregate_writes (une ligne par écriture),
    car sales_aggregates.updated_at est écrasé à chaque upsert.
    """
    return percentiles(f"""
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY l), percentile_cont(0.95) WITHIN GROUP (ORDER BY l),
               max(l), count(*)
        FROM (SELECT extract(epoch FROM written_at - max_event_time) l FROM aggregate_writes
              WHERE written_at BETWEEN '{t0}' AND '{t1}') x""")


def container_logs(name):
    proc = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    return proc.stdout + proc.stderr


def run(config, duration, warmup, run_id):
    partitions, rate, master = config["partitions"], config["rate"], config["master"]
    suffix = run_id.replace("-", "")[-12:]
    orders_topic, events_topic = f"bench-orders-{suffix}", f"bench-events-{suffix}"
    env = dict(ENV,
               TOPIC_ORDERS=orders_topic, TOPIC_ORDER_EVENTS=events_topic,
               EVENT_TOPIC_PARTITIONS=str(partitions),
               CHECKPOINT_DIR=f"/checkpoints/bench-{run_id}",
               SPARK_MASTER=master, RATE=str(rate))
    label = f"rate={rate} cmd/s, partitions={partitions}, spark={master}"
    print(f"\n>>> {label}", flush=True)

    sh("docker", "compose", "stop", "producer", "spark", env=env)
    # Topics dédiés au run (aucune donnée d'un run précédent n'est relue).
    sh("docker", "compose", "run", "--rm", "-T", "tools", "python", "-c",
       "from common.kafka_utils import ensure_topics; ensure_topics()", env=env)
    # Spark d'abord, producteur une fois les requêtes démarrées.
    sh("docker", "compose", "up", "-d", "--no-deps", "spark", env=env)
    while "Requêtes démarrées" not in container_logs("retailpulse-spark"):
        time.sleep(3)
    sh("docker", "compose", "up", "-d", "--no-deps", "producer", env=env)
    time.sleep(warmup)

    t0 = datetime.now(timezone.utc)
    sampler = StatsSampler()
    sampler.start()
    time.sleep(duration)
    t1 = datetime.now(timezone.utc)
    sampler.stop_event.set()
    sh("docker", "compose", "stop", "-t", "20", "producer", env=env)
    stop_time = time.monotonic()

    window = f"ingested_at BETWEEN '{t0.isoformat()}' AND '{t1.isoformat()}'"
    topics = f"kafka_topic IN ('{orders_topic}', '{events_topic}')"

    # Drain : temps jusqu'à ce que tous les événements produits soient en base.
    summaries = [json.loads(m) for m in
                 re.findall(r'(\{"orders".*\})', container_logs("retailpulse-producer"))]
    produced = summaries[-1] if summaries else {}
    target = produced.get("delivered", 0)
    drain_s = None
    for _ in range(150):
        count = scalar(f"SELECT count(*) FROM order_events WHERE {topics}") or 0
        invalid = scalar(f"SELECT count(*) FROM invalid_events WHERE {topics}") or 0
        if count + invalid >= target:
            drain_s = round(time.monotonic() - stop_time, 1)
            break
        time.sleep(2)

    processed = scalar(f"SELECT count(*) FROM order_events WHERE {topics} AND {window}") or 0
    result = {
        "label": label, **config, "run_id": run_id,
        "measure_window": [t0.isoformat(), t1.isoformat()],
        "produced_events_per_s": produced.get("events_per_s"),
        "produced_events_total": target,
        "processed_events_per_s": round(processed / duration, 2),
        "drain_after_stop_s": drain_s,
        "latency_to_order_events_s": percentiles(f"""
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY l), percentile_cont(0.95) WITHIN GROUP (ORDER BY l),
                   max(l), count(*)
            FROM (SELECT extract(epoch FROM ingested_at - event_timestamp) l FROM order_events
                  WHERE {topics} AND {window}) x"""),
        "latency_to_aggregate_s": aggregate_latency(t0.isoformat(), t1.isoformat()),
        "spark_batches": {
            row[0]: {"batches": int(row[1]), "avg_duration_ms": round(float(row[2])),
                     "max_duration_ms": int(row[3]), "avg_input_rows": round(float(row[4]), 1),
                     "avg_processed_rows_per_s": round(float(row[5] or 0), 1)}
            for row in sql(f"""
                SELECT query_name, count(*), avg(batch_duration_ms), max(batch_duration_ms),
                       avg(num_input_rows), avg(processed_rows_per_s)
                FROM stream_progress
                WHERE batch_timestamp BETWEEN '{t0.isoformat()}' AND '{t1.isoformat()}'
                GROUP BY query_name""")
        },
        "resources": sampler.summary(),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False), flush=True)
    return result


def to_markdown(results):
    lines = [
        "| Configuration | Produit (évt/s) | Traité (évt/s) | Latence → order_events p50 / p95 (s) "
        "| Latence → agrégat p50 / p95 (s) | Batch agrégats moy. / max (ms) | Drain (s) "
        "| CPU Spark moy. / max (%) | RAM Spark max (MiB) | CPU Kafka moy. (%) |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lo, la = r["latency_to_order_events_s"], r["latency_to_aggregate_s"]
        agg = r["spark_batches"].get("sales-aggregates", {})
        spark = r["resources"].get("spark", {})
        kafka = r["resources"].get("kafka", {})
        lines.append(
            f"| {r['label']} | {r['produced_events_per_s']} | {r['processed_events_per_s']} "
            f"| {lo['p50']} / {lo['p95']} | {la['p50']} / {la['p95']} "
            f"| {agg.get('avg_duration_ms')} / {agg.get('max_duration_ms')} | {r['drain_after_stop_s']} "
            f"| {spark.get('cpu_avg_pct')} / {spark.get('cpu_max_pct')} | {spark.get('mem_max_mib')} "
            f"| {kafka.get('cpu_avg_pct')} |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiments", default="rate,partitions,cores")
    parser.add_argument("--duration", type=int, default=90)
    parser.add_argument("--warmup", type=int, default=25)
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    results, done = [], {}
    for name in args.experiments.split(","):
        for i, config in enumerate(EXPERIMENTS[name]):
            key = (config["rate"], config["partitions"], config["master"])
            if key in done:            # configuration partagée entre deux expériences
                results.append({**done[key], "experiment": name})
                continue
            result = {**run(config, args.duration, args.warmup, f"{stamp}-{name}-{i}"),
                      "experiment": name}
            done[key] = result
            results.append(result)

    # Retour à la configuration nominale.
    sh("docker", "compose", "up", "-d", "--no-deps", "spark", "producer")

    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / f"benchmark-{stamp}.json").write_text(json.dumps(results, indent=2, ensure_ascii=False))
    write_markdown(results, RESULTS / f"benchmark-{stamp}.md")
    print(f"\nRésultats : docs/results/benchmark-{stamp}.json / .md")


def write_markdown(results, path):
    md = []
    for name in dict.fromkeys(r["experiment"] for r in results):
        md += [f"\n### Expérience : {name}\n", to_markdown([r for r in results if r["experiment"] == name])]
    path.write_text("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
