import json
import logging
import math
import os
import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
# Silence the verbose per-connection INFO logs from the Kafka client library.
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
PG_HOST        = os.getenv("PG_HOST", "postgresql")
PG_PORT        = int(os.getenv("PG_PORT", "5432"))
PG_DB          = os.getenv("PG_DB", "transactions")
PG_USER        = os.getenv("PG_USER", "postgres")
PG_PASSWORD    = os.getenv("PG_PASSWORD", "postgres")

TOPIC           = "transactions"
CONSUMER_GROUP  = "anomaly-detection-group"
WINDOW_SIZE     = 20   # max historical amounts per card for z-score
MIN_WINDOW      = 3    # minimum samples before z-score is meaningful
Z_THRESHOLD     = 3.0  # standard deviations above mean to flag anomaly
METRICS_INTERVAL = 10  # seconds between PostgreSQL metric writes
MONGO_BATCH_SIZE = 50  # documents to buffer before flushing to MongoDB
LOG_EVERY_N     = 100  # log progress every N transactions

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("[CONSUMER] Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def build_kafka_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            logger.info("[CONSUMER] Connected to Kafka at %s", KAFKA_BROKER)
            return consumer
        except NoBrokersAvailable:
            logger.warning("[CONSUMER] Kafka unavailable — retrying in 5s")
            time.sleep(5)


def build_mongo_collection():
    """Connect to MongoDB and ensure the indexes the dashboard relies on exist.

    MongoClient connects lazily, so we force a ping and retry until the server
    actually answers — mirroring the Kafka and PostgreSQL connection helpers.
    """
    while True:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            collection = client["transactions"]["raw_events"]

            collection.create_index([("timestamp", DESCENDING)])
            collection.create_index([("anomaly_flag", ASCENDING), ("timestamp", DESCENDING)])
            collection.create_index("card_id")
            collection.create_index("merchant_category")

            logger.info("[CONSUMER] Connected to MongoDB at %s", MONGO_URI)
            return client, collection
        except PyMongoError:
            logger.warning("[CONSUMER] MongoDB unavailable — retrying in 5s")
            time.sleep(5)


def build_pg_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            logger.info("[CONSUMER] Connected to PostgreSQL at %s:%d", PG_HOST, PG_PORT)
            return conn
        except psycopg2.OperationalError:
            logger.warning("[CONSUMER] PostgreSQL unavailable — retrying in 5s")
            time.sleep(5)


def compute_z_score(window: deque, amount: float) -> tuple[bool, float | None]:
    """
    Compare amount against the card's recent history using a z-score.

    Uses sample standard deviation (ddof=1) which is statistically appropriate
    for a finite sliding window. Returns (False, None) when the window is too
    small or has zero variance.
    """
    if len(window) < MIN_WINDOW:
        return False, None

    n = len(window)
    mean = sum(window) / n
    variance = sum((x - mean) ** 2 for x in window) / (n - 1)
    std_dev = math.sqrt(variance)

    if std_dev == 0:
        return False, None

    z = (amount - mean) / std_dev
    return z > Z_THRESHOLD, round(z, 4)


def flush_mongo(collection, batch: list) -> None:
    if not batch:
        return
    try:
        collection.insert_many(batch, ordered=False)
    except PyMongoError as exc:
        logger.error("[CONSUMER] MongoDB insert failed: %s", exc)


_UPSERT_SQL = """
    INSERT INTO metrics (
        aggregation_time, total_transactions, total_anomalies, anomaly_rate,
        average_amount, max_amount, min_amount, merchant_category_breakdown
    ) VALUES (
        %(aggregation_time)s, %(total_transactions)s, %(total_anomalies)s,
        %(anomaly_rate)s, %(average_amount)s, %(max_amount)s, %(min_amount)s,
        %(merchant_category_breakdown)s
    )
    ON CONFLICT (aggregation_time) DO UPDATE SET
        total_transactions          = EXCLUDED.total_transactions,
        total_anomalies             = EXCLUDED.total_anomalies,
        anomaly_rate                = EXCLUDED.anomaly_rate,
        average_amount              = EXCLUDED.average_amount,
        max_amount                  = EXCLUDED.max_amount,
        min_amount                  = EXCLUDED.min_amount,
        merchant_category_breakdown = EXCLUDED.merchant_category_breakdown
"""


def flush_metrics(pg_conn, window: list) -> None:
    if not window:
        return

    amounts = [t["amount"] for t in window]
    total = len(window)
    anomalies = sum(1 for t in window if t["anomaly_flag"])
    breakdown: dict[str, int] = {}
    for t in window:
        cat = t["merchant_category"]
        breakdown[cat] = breakdown.get(cat, 0) + 1

    row = {
        "aggregation_time": datetime.now(timezone.utc),
        "total_transactions": total,
        "total_anomalies": anomalies,
        "anomaly_rate": round((anomalies / total) * 100, 4),
        "average_amount": round(sum(amounts) / len(amounts), 4),
        "max_amount": max(amounts),
        "min_amount": min(amounts),
        "merchant_category_breakdown": json.dumps(breakdown),
    }

    try:
        with pg_conn.cursor() as cur:
            cur.execute(_UPSERT_SQL, row)
        pg_conn.commit()
        logger.info(
            "[METRICS] 10s window: %d transactions, %d anomalies, rate: %.2f%%",
            row["total_transactions"],
            row["total_anomalies"],
            row["anomaly_rate"],
        )
    except psycopg2.Error as exc:
        logger.error("[CONSUMER] PostgreSQL insert failed: %s", exc)
        pg_conn.rollback()


def run() -> None:
    consumer = build_kafka_consumer()
    mongo_client, mongo_collection = build_mongo_collection()
    pg_conn = build_pg_connection()

    # One sliding window of recent amounts per card, capped at WINDOW_SIZE.
    card_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

    mongo_batch: list = []
    metrics_window: list = []
    last_metrics_flush = time.monotonic()
    total_processed = 0

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000)

            for _, messages in records.items():
                for msg in messages:
                    txn = msg.value
                    try:
                        card_id = txn["card_id"]
                        amount = float(txn["amount"])
                    except (KeyError, TypeError, ValueError) as exc:
                        logger.warning(
                            "[CONSUMER] Skipping malformed message: %s (%s)", txn, exc
                        )
                        continue

                    is_anomaly, z = compute_z_score(card_windows[card_id], amount)
                    card_windows[card_id].append(amount)

                    if is_anomaly:
                        logger.info(
                            "[ANOMALY] %s | card: %s | amount: $%.2f | z_score: %.4f",
                            txn["transaction_id"],
                            card_id,
                            amount,
                            z,
                        )

                    enriched = {
                        "transaction_id": txn["transaction_id"],
                        "timestamp": txn["timestamp"],
                        "amount": amount,
                        "merchant_category": txn["merchant_category"],
                        "card_id": card_id,
                        "anomaly_flag": is_anomaly,
                        "z_score": z,
                        "created_at": datetime.now(timezone.utc),
                    }
                    mongo_batch.append(enriched)
                    metrics_window.append(enriched)
                    total_processed += 1

                    if total_processed % LOG_EVERY_N == 0:
                        logger.info("[CONSUMER] Processed %d transactions", total_processed)

                    if len(mongo_batch) >= MONGO_BATCH_SIZE:
                        flush_mongo(mongo_collection, mongo_batch)
                        mongo_batch.clear()

            # Tumbling metrics window: flush an aggregated row every METRICS_INTERVAL.
            now = time.monotonic()
            if now - last_metrics_flush >= METRICS_INTERVAL:
                flush_metrics(pg_conn, metrics_window)
                metrics_window.clear()
                last_metrics_flush = now

    except KeyboardInterrupt:
        logger.info("[CONSUMER] Keyboard interrupt received")
    finally:
        flush_mongo(mongo_collection, mongo_batch)
        flush_metrics(pg_conn, metrics_window)
        consumer.close()
        mongo_client.close()
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
        logger.info("[CONSUMER] Shutting down gracefully")


if __name__ == "__main__":
    run()
