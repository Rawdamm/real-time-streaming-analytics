import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
# Kafka's client logs every broker connection at INFO, which drowns out our own
# messages. Keep it at WARNING so the producer log stays readable.
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

_running = True


def _handle_shutdown(signum, frame):
    """Stop the produce loop cleanly when Docker sends SIGTERM/SIGINT."""
    global _running
    logger.info("[PRODUCER] Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "transactions"
PRODUCE_INTERVAL = 0.2  # seconds — 5 messages per second
ANOMALY_INTERVAL = 50   # inject anomaly every Nth transaction

MERCHANT_CATEGORIES = [
    "grocery",
    "electronics",
    "restaurant",
    "gas_station",
    "entertainment",
    "healthcare",
    "utilities",
    "travel",
    "retail",
    "other",
]

CARD_POOL = [f"CARD_{i:05d}" for i in random.sample(range(10_000), 100)]


def build_producer() -> KafkaProducer:
    """Create and return a KafkaProducer, retrying until the broker is reachable."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("[PRODUCER] Connected to Kafka broker at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "[PRODUCER] Kafka broker unavailable at %s — retrying in 5s",
                KAFKA_BROKER,
            )
            time.sleep(5)


def generate_transaction(count: int) -> dict:
    """Return a single transaction record, injecting an anomaly on every 50th call."""
    is_anomaly = count % ANOMALY_INTERVAL == 0 and count > 0
    amount = (
        round(random.uniform(500, 2000), 2)
        if is_anomaly
        else round(max(0.01, random.gauss(50, 30)), 2)
    )
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "amount": amount,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "card_id": random.choice(CARD_POOL),
        "is_injected_anomaly": is_anomaly,
    }


def run(producer: KafkaProducer) -> None:
    """Produce transactions at PRODUCE_INTERVAL seconds each until shut down."""
    count = 0
    while _running:
        count += 1
        txn = generate_transaction(count)

        try:
            producer.send(TOPIC, value=txn)
            if txn["is_injected_anomaly"]:
                logger.info(
                    "[PRODUCER] Anomaly injected #%d | card: %s | amount: $%.2f",
                    count,
                    txn["card_id"],
                    txn["amount"],
                )
            elif count % 50 == 1:
                logger.info("[PRODUCER] Sent transaction #%d", count)

            # Periodic flush bounds how many messages can be lost on a crash.
            if count % 10 == 0:
                producer.flush()
        except Exception as exc:
            logger.error("[PRODUCER] Failed to send message #%d: %s", count, exc)

        time.sleep(PRODUCE_INTERVAL)


def main() -> None:
    producer = build_producer()
    try:
        run(producer)
    finally:
        producer.flush()
        producer.close()
        logger.info("[PRODUCER] KafkaProducer closed")


if __name__ == "__main__":
    main()
