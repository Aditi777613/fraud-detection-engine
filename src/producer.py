import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.confluent_config import (
    PRODUCER_CONFIG,
    TOPIC_TRANSACTIONS,
    TOPIC_ALERTS
)
from src.utils import setup_logger, serialize_transaction

logger = setup_logger('TransactionProducer')


class TransactionGenerator:
    """Generate realistic transaction data"""

    def __init__(self):
        self.merchants = [
            'Amazon', 'Walmart', 'Target', 'Starbucks', 'McDonalds',
            'Shell Gas', 'Netflix', 'Spotify', 'Apple Store', 'Best Buy'
        ]

        self.merchant_categories = [
            'retail', 'food', 'fuel', 'entertainment', 'electronics',
            'grocery', 'online', 'subscription', 'travel', 'utilities'
        ]

        self.locations = [
            'New York, NY', 'Los Angeles, CA', 'Chicago, IL',
            'Houston, TX', 'Phoenix, AZ', 'Miami, FL'
        ]

        self.user_ids = [f"USER_{i:04d}" for i in range(1, 101)]

    def generate_transaction(self, fraud_probability=0.1):
        """Generate a single transaction"""
        is_fraud = random.random() < fraud_probability

        amount = (
            round(random.uniform(500, 5000), 2)
            if is_fraud
            else round(random.uniform(5, 500), 2)
        )

        transaction = {
            "transaction_id": f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            "user_id": random.choice(self.user_ids),
            "amount": amount,
            "merchant": random.choice(self.merchants),
            "merchant_category": random.choice(self.merchant_categories),
            "location": random.choice(self.locations),
            "timestamp": datetime.utcnow().isoformat(),
            "is_fraud": is_fraud,
            "device_id": f"DEVICE_{random.randint(1000, 9999)}",
            "ip_address": ".".join(str(random.randint(1, 255)) for _ in range(4)),
        }

        return transaction


class TransactionProducer:
    """Kafka producer for transactions and fraud alerts"""

    def __init__(self):
        self.producer = Producer(PRODUCER_CONFIG)
        self.generator = TransactionGenerator()
        logger.info("✅ Transaction Producer initialized")

    def delivery_callback(self, err, msg):
        if err:
            logger.error(f"❌ Delivery failed: {err}")
        else:
            logger.info(f"📨 Delivered to {msg.topic()} [{msg.partition()}]")

    def produce_transaction(self, transaction):
        """Send transaction to Kafka"""
        self.producer.produce(
            topic=TOPIC_TRANSACTIONS,
            key=transaction["transaction_id"].encode(),
            value=serialize_transaction(transaction),
            callback=self.delivery_callback,
        )
        self.producer.poll(0)

    def produce_fraud_alert(self, transaction):
        """Send fraud alert to Kafka"""
        alert = {
            "alert_id": f"ALERT_{transaction['transaction_id']}",
            "transaction": transaction,
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "high" if transaction["amount"] > 1000 else "medium",
            "reason": "rule-based-detection",
        }

        self.producer.produce(
            topic=TOPIC_ALERTS,
            key=transaction["transaction_id"].encode(),
            value=json.dumps(alert).encode(),
            callback=self.delivery_callback,
        )
        self.producer.poll(0)

    def run(self, interval=2, fraud_probability=0.15):
        """Run producer forever"""
        logger.info(
            f"🚀 Streaming started | interval={interval}s | fraud_rate={fraud_probability * 100}%"
        )

        transaction_count = 0
        fraud_count = 0

        try:
            while True:
                transaction = self.generator.generate_transaction(fraud_probability)

                self.produce_transaction(transaction)
                transaction_count += 1

                if transaction["is_fraud"]:
                    fraud_count += 1
                    self.produce_fraud_alert(transaction)

                if transaction_count % 10 == 0:
                    logger.info(
                        f"📊 Produced {transaction_count} txns ({fraud_count} fraud)"
                    )

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("🛑 Producer stopped by user")

        finally:
            self.producer.flush()
            logger.info("✅ Kafka producer flushed & closed")


if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run()
