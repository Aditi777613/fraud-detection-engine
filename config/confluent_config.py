import os
from dotenv import load_dotenv

load_dotenv()

# Confluent Cloud Configuration
CONFLUENT_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
}

# Producer Configuration
PRODUCER_CONFIG = {
    **CONFLUENT_CONFIG,
    'client.id': 'fraud-detection-producer',
    'acks': 'all',
    'retries': 3,
    'max.in.flight.requests.per.connection': 5,
    'compression.type': 'snappy',
}

# Consumer Configuration
CONSUMER_CONFIG = {
    **CONFLUENT_CONFIG,
    'group.id': 'fraud-detection-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'session.timeout.ms': 45000,
}

# Kafka Topics
TOPIC_TRANSACTIONS = os.getenv('KAFKA_TOPIC_TRANSACTIONS', 'transactions')
TOPIC_ALERTS = os.getenv('KAFKA_TOPIC_ALERTS', 'fraud-alerts')
TOPIC_METRICS = os.getenv('KAFKA_TOPIC_METRICS', 'metrics')

def validate_config():
    """Validate that all required configuration is present"""
    required_vars = [
        'CONFLUENT_BOOTSTRAP_SERVERS',
        'CONFLUENT_API_KEY',
        'CONFLUENT_API_SECRET'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    print("✓ Confluent configuration validated")

if __name__ == '__main__':
    validate_config()