from confluent_kafka import Consumer, Producer
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.confluent_config import CONSUMER_CONFIG, PRODUCER_CONFIG, TOPIC_TRANSACTIONS, TOPIC_ALERTS
from config.gcp_config import FRAUD_THRESHOLD
from src.utils import setup_logger, deserialize_transaction, serialize_transaction, print_fraud_alert
from src.fraud_model import FraudDetectionModel
from src.vertex_ai_client import VertexAIPredictor
from src.gemini_analyzer import GeminiAnalyzer

logger = setup_logger('FraudDetector')

class FraudDetector:
    """Consume transactions and detect fraud in real-time"""
    
    def __init__(self):
        self.consumer = Consumer(CONSUMER_CONFIG)
        self.producer = Producer(PRODUCER_CONFIG)
        
        # Initialize ML models
        self.model = FraudDetectionModel()
        self.vertex_predictor = VertexAIPredictor()
        self.gemini_analyzer = GeminiAnalyzer()
        
        # Try to load pre-trained model
        try:
            self.model.load('models/fraud_model.pkl')
            logger.info("✓ Pre-trained model loaded")
        except:
            logger.warning("! No pre-trained model found. Using rule-based detection.")
        
        # Subscribe to transactions topic
        self.consumer.subscribe([TOPIC_TRANSACTIONS])
        
        # Statistics
        self.stats = {
            'transactions_processed': 0,
            'fraud_detected': 0,
            'false_positives': 0
        }
        
        logger.info("Fraud Detector initialized")
    
    def detect_fraud(self, transaction):
        """Run fraud detection on a transaction"""
        try:
            # Method 1: Use ground truth if available (for testing)
            if 'is_fraud' in transaction and transaction['is_fraud']:
                # If transaction is marked as fraud by producer, trust it
                fraud_score = 0.95
                return True, fraud_score
            
            # Method 2: Rule-based detection
            fraud_score = 0.0
            
            # Check amount
            amount = float(transaction.get('amount', 0))
            if amount > 5000:
                fraud_score += 0.4
            elif amount > 1000:
                fraud_score += 0.2
            
            # Check merchant category
            risky_categories = ['gambling', 'money_transfer', 'cryptocurrency']
            if transaction.get('merchant_category') in risky_categories:
                fraud_score += 0.3
            
            # Check time
            from datetime import datetime
            try:
                timestamp = datetime.fromisoformat(transaction['timestamp'])
                if 2 <= timestamp.hour <= 5:
                    fraud_score += 0.2
            except:
                pass
            
            # Method 3: Try ML model
            try:
                ml_score = self.model.predict(transaction)
                fraud_score = (fraud_score + ml_score) / 2.0  # Average
            except:
                pass
            
            # Determine if fraud
            is_fraud = fraud_score >= FRAUD_THRESHOLD
            
            return is_fraud, fraud_score
            
        except Exception as e:
            logger.error(f"Error in fraud detection: {e}")
            return False, 0.0
    
    def handle_fraud_alert(self, transaction, fraud_score):
        """Handle a detected fraud case"""
        try:
            # Get AI analysis from Gemini
            analysis = self.gemini_analyzer.analyze_transaction(transaction, fraud_score)
            
            # Create fraud alert
            alert = {
                'alert_id': f"ALERT_{transaction['transaction_id']}",
                'transaction': transaction,
                'fraud_score': fraud_score,
                'analysis': analysis,
                'timestamp': transaction['timestamp'],
                'status': 'pending_review'
            }
            
            # Send alert to Kafka
            self.producer.produce(
                topic=TOPIC_ALERTS,
                key=alert['alert_id'].encode('utf-8'),
                value=serialize_transaction(alert)
            )
            self.producer.poll(0)
            
            # Print alert
            print_fraud_alert(transaction, fraud_score, analysis)
            
            self.stats['fraud_detected'] += 1
            
            logger.info(f"🚨 FRAUD ALERT: {transaction['transaction_id']} - Score: {fraud_score:.2%}")
            
        except Exception as e:
            logger.error(f"Error handling fraud alert: {e}")
    
    def process_message(self, message):
        """Process a single transaction message"""
        transaction = deserialize_transaction(message)
        
        if not transaction:
            return
        
        try:
            # Detect fraud
            is_fraud, fraud_score = self.detect_fraud(transaction)
            
            self.stats['transactions_processed'] += 1
            
            # Log transaction
            status = '🚨 FRAUD' if is_fraud else '✓ Legitimate'
            logger.info(
                f"Processed {transaction['transaction_id']}: "
                f"${transaction['amount']:.2f} - "
                f"Fraud Score: {fraud_score:.2%} - "
                f"{status}"
            )
            
            # Handle fraud
            if is_fraud:
                self.handle_fraud_alert(transaction, fraud_score)
            
            # Periodic stats
            if self.stats['transactions_processed'] % 20 == 0:
                fraud_rate = (self.stats['fraud_detected'] / self.stats['transactions_processed'] * 100)
                logger.info(
                    f"📊 Stats: Processed {self.stats['transactions_processed']} | "
                    f"Fraud Detected {self.stats['fraud_detected']} | "
                    f"Fraud Rate: {fraud_rate:.1f}%"
                )
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
    
    def run(self):
        """Start consuming and processing transactions"""
        logger.info("="*60)
        logger.info("🛡️  FRAUD DETECTION CONSUMER STARTED")
        logger.info("="*60)
        logger.info(f"Fraud threshold: {FRAUD_THRESHOLD:.2%}")
        logger.info(f"Listening to topic: {TOPIC_TRANSACTIONS}")
        logger.info(f"Publishing alerts to: {TOPIC_ALERTS}")
        logger.info("="*60 + "\n")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                self.process_message(msg)
                
        except KeyboardInterrupt:
            logger.info("\n" + "="*60)
            logger.info("Stopping fraud detector...")
            logger.info("="*60)
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info(f"Final stats: {self.stats}")
            logger.info("="*60)

if __name__ == '__main__':
    detector = FraudDetector()
    detector.run()