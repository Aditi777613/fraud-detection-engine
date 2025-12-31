import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.confluent_config import PRODUCER_CONFIG, TOPIC_TRANSACTIONS
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
        
        user_id = random.choice(self.user_ids)
        merchant = random.choice(self.merchants)
        category = random.choice(self.merchant_categories)
        location = random.choice(self.locations)
        
        # Normal transactions: $5 - $500
        # Fraudulent transactions: Often higher amounts
        if is_fraud:
            amount = round(random.uniform(500, 5000), 2)
        else:
            amount = round(random.uniform(5, 500), 2)
        
        transaction = {
            'transaction_id': f"TXN_{int(time.time()*1000)}_{random.randint(1000, 9999)}",
            'user_id': user_id,
            'amount': amount,
            'merchant': merchant,
            'merchant_category': category,
            'location': location,
            'timestamp': datetime.now().isoformat(),
            'is_fraud': is_fraud,  # Ground truth for training
            'device_id': f"DEVICE_{random.randint(1000, 9999)}",
            'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        }
        
        return transaction

class TransactionProducer:
    """Kafka producer for transactions"""
    
    def __init__(self):
        self.producer = Producer(PRODUCER_CONFIG)
        self.generator = TransactionGenerator()
        logger.info("Transaction Producer initialized")
        
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce_transaction(self, transaction):
        """Produce a transaction to Kafka"""
        try:
            self.producer.produce(
                topic=TOPIC_TRANSACTIONS,
                key=transaction['transaction_id'].encode('utf-8'),
                value=serialize_transaction(transaction),
                callback=self.delivery_callback
            )
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error producing transaction: {e}")
    
    def run(self, interval=2, fraud_probability=0.1):
        """Run the producer continuously"""
        logger.info(f"Starting transaction stream (interval: {interval}s, fraud rate: {fraud_probability*100}%)")
        
        try:
            transaction_count = 0
            fraud_count = 0
            
            while True:
                transaction = self.generator.generate_transaction(fraud_probability)
                self.produce_transaction(transaction)
                
                transaction_count += 1
                if transaction['is_fraud']:
                    fraud_count += 1
                
                if transaction_count % 10 == 0:
                    logger.info(f"Produced {transaction_count} transactions ({fraud_count} fraudulent)")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        finally:
            self.producer.flush()
            logger.info(f"Total transactions produced: {transaction_count}")

if __name__ == '__main__':
    producer = TransactionProducer()
    producer.run(interval=2, fraud_probability=0.15)
