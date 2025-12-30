import numpy as np
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import joblib
import os

class FraudDetectionModel:
    """Simple fraud detection model"""
    
    def __init__(self):
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        self.label_encoders = {}
        self.feature_names = [
            'amount', 'hour', 'day_of_week', 'merchant_category_encoded',
            'location_encoded', 'amount_zscore'
        ]
        self.is_trained = False
        
    def extract_features(self, transaction):
        """Extract features from transaction"""
        timestamp = datetime.fromisoformat(transaction['timestamp'])
        
        features = {
            'amount': float(transaction['amount']),
            'hour': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'merchant_category': transaction.get('merchant_category', 'unknown'),
            'location': transaction.get('location', 'unknown'),
        }
        
        # Encode categorical features
        if 'merchant_category' in features:
            if 'merchant_category' not in self.label_encoders:
                self.label_encoders['merchant_category'] = LabelEncoder()
                self.label_encoders['merchant_category'].fit(['unknown'])
            
            try:
                features['merchant_category_encoded'] = self.label_encoders['merchant_category'].transform([features['merchant_category']])[0]
            except:
                features['merchant_category_encoded'] = 0
        
        if 'location' in features:
            if 'location' not in self.label_encoders:
                self.label_encoders['location'] = LabelEncoder()
                self.label_encoders['location'].fit(['unknown'])
            
            try:
                features['location_encoded'] = self.label_encoders['location'].transform([features['location']])[0]
            except:
                features['location_encoded'] = 0
        
        # Z-score for amount (simple normalization)
        features['amount_zscore'] = (features['amount'] - 250) / 500
        
        return features
    
    def prepare_features_array(self, features):
        """Convert features dict to array"""
        return np.array([
            features['amount'],
            features['hour'],
            features['day_of_week'],
            features['merchant_category_encoded'],
            features['location_encoded'],
            features['amount_zscore']
        ]).reshape(1, -1)
    
    def train(self, transactions):
        """Train the model on historical transactions"""
        if len(transactions) < 10:
            print("Not enough data to train")
            return
        
        X = []
        y = []
        
        # First pass: fit label encoders
        for txn in transactions:
            features = self.extract_features(txn)
            
            if 'merchant_category' in features:
                if 'merchant_category' not in self.label_encoders:
                    self.label_encoders['merchant_category'] = LabelEncoder()
                categories = [t.get('merchant_category', 'unknown') for t in transactions]
                self.label_encoders['merchant_category'].fit(categories)
            
            if 'location' in features:
                if 'location' not in self.label_encoders:
                    self.label_encoders['location'] = LabelEncoder()
                locations = [t.get('location', 'unknown') for t in transactions]
                self.label_encoders['location'].fit(locations)
        
        # Second pass: extract features
        for txn in transactions:
            features = self.extract_features(txn)
            feature_array = self.prepare_features_array(features)
            X.append(feature_array[0])
            y.append(1 if txn.get('is_fraud', False) else 0)
        
        X = np.array(X)
        y = np.array(y)
        
        self.model.fit(X, y)
        self.is_trained = True
        print(f"Model trained on {len(X)} transactions")
        
    def predict(self, transaction):
        """Predict fraud probability for a transaction"""
        if not self.is_trained:
            # Return random score if not trained
            return np.random.uniform(0.3, 0.7)
        
        features = self.extract_features(transaction)
        X = self.prepare_features_array(features)
        
        try:
            fraud_probability = self.model.predict_proba(X)[0][1]
            return float(fraud_probability)
        except:
            return 0.5
    
    def save(self, filepath='models/fraud_model.pkl'):
        """Save model to disk"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        joblib.dump({
            'model': self.model,
            'label_encoders': self.label_encoders,
            'is_trained': self.is_trained
        }, filepath)
        print(f"Model saved to {filepath}")
    
    def load(self, filepath='models/fraud_model.pkl'):
        """Load model from disk"""
        if os.path.exists(filepath):
            data = joblib.load(filepath)
            self.model = data['model']
            self.label_encoders = data['label_encoders']
            self.is_trained = data['is_trained']
            print(f"Model loaded from {filepath}")
        else:
            print(f"Model file not found: {filepath}")

if __name__ == '__main__':
    # Test the model
    model = FraudDetectionModel()
    
    test_transaction = {
        'transaction_id': 'TEST_001',
        'amount': 5000,
        'merchant_category': 'gambling',
        'location': 'Unknown',
        'timestamp': datetime.now().isoformat(),
        'is_fraud': True
    }
    
    score = model.predict(test_transaction)
    print(f"Fraud score: {score:.2%}")