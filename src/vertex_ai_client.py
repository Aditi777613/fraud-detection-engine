from google.cloud import aiplatform
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.gcp_config import GCP_PROJECT_ID
from src.utils import setup_logger

logger = setup_logger('VertexAIClient')

class VertexAIPredictor:
    """Client for Vertex AI predictions"""
    
    def __init__(self):
        try:
            aiplatform.init(
                project=GCP_PROJECT_ID,
                location=VERTEX_AI_LOCATION
            )
            self.endpoint = None
            if VERTEX_AI_ENDPOINT:
                self.endpoint = aiplatform.Endpoint(VERTEX_AI_ENDPOINT)
                logger.info(f"Connected to Vertex AI endpoint: {VERTEX_AI_ENDPOINT}")
            else:
                logger.warning("No Vertex AI endpoint configured. Using local model fallback.")
        except Exception as e:
            logger.error(f"Error initializing Vertex AI: {e}")
            self.endpoint = None
    
    def predict_fraud(self, transaction):
        """Get fraud prediction from Vertex AI"""
        if not self.endpoint:
            # Fallback to rule-based scoring
            return self._fallback_prediction(transaction)
        
        try:
            # Prepare features for Vertex AI
            features = self._prepare_features(transaction)
            
            # Get prediction
            prediction = self.endpoint.predict(instances=[features])
            
            # Extract fraud probability
            fraud_score = prediction.predictions[0][1]
            
            logger.info(f"Vertex AI prediction for {transaction['transaction_id']}: {fraud_score:.2%}")
            return fraud_score
            
        except Exception as e:
            logger.error(f"Error getting Vertex AI prediction: {e}")
            return self._fallback_prediction(transaction)
    
    def _prepare_features(self, transaction):
        """Prepare transaction features for Vertex AI"""
        from datetime import datetime
        
        timestamp = datetime.fromisoformat(transaction['timestamp'])
        
        return {
            'amount': float(transaction['amount']),
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'merchant_category': transaction.get('merchant_category', 'unknown'),
            'location': transaction.get('location', 'unknown')
        }
    
    def _fallback_prediction(self, transaction):
        """Simple rule-based fraud scoring when Vertex AI is unavailable"""
        score = 0.0
        
        # High amount
        if transaction['amount'] > 1000:
            score += 0.3
        
        # Very high amount
        if transaction['amount'] > 5000:
            score += 0.3
        
        # Suspicious merchant categories
        risky_categories = ['gambling', 'money_transfer', 'cryptocurrency']
        if transaction.get('merchant_category') in risky_categories:
            score += 0.2
        
        # Odd hours (2 AM - 5 AM)
        from datetime import datetime
        timestamp = datetime.fromisoformat(transaction['timestamp'])
        if 2 <= timestamp.hour <= 5:
            score += 0.2
        
        return min(score, 1.0)
    
    def batch_predict(self, transactions):
        """Batch prediction for multiple transactions"""
        return [self.predict_fraud(txn) for txn in transactions]

if __name__ == '__main__':
    # Test the client
    from datetime import datetime
    
    predictor = VertexAIPredictor()
    
    test_transaction = {
        'transaction_id': 'TEST_001',
        'amount': 5000,
        'merchant_category': 'gambling',
        'location': 'Unknown',
        'timestamp': datetime.now().isoformat()
    }
    
    score = predictor.predict_fraud(test_transaction)
    print(f"Fraud score: {score:.2%}")