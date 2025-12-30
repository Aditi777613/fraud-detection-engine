import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fraud_model import FraudDetectionModel
from src.producer import TransactionGenerator
from datetime import datetime
import random

def generate_training_data(num_samples=1000):
    """Generate synthetic training data"""
    print(f"Generating {num_samples} training samples...")
    
    generator = TransactionGenerator()
    transactions = []
    
    for i in range(num_samples):
        # Generate with 20% fraud rate
        transaction = generator.generate_transaction(fraud_probability=0.2)
        transactions.append(transaction)
        
        if (i + 1) % 100 == 0:
            print(f"Generated {i + 1}/{num_samples} samples")
    
    fraud_count = sum(1 for t in transactions if t['is_fraud'])
    print(f"✓ Generated {num_samples} transactions ({fraud_count} fraudulent, {fraud_count/num_samples*100:.1f}%)")
    
    return transactions

def train_and_save_model():
    """Train the fraud detection model"""
    print("Starting model training...")
    print("="*60)
    
    # Generate training data
    transactions = generate_training_data(num_samples=2000)
    
    # Initialize and train model
    model = FraudDetectionModel()
    model.train(transactions)
    
    # Test the model
    print("\n" + "="*60)
    print("Testing model...")
    
    test_transactions = [
        {
            'transaction_id': 'TEST_001',
            'amount': 50,
            'merchant_category': 'grocery',
            'location': 'New York, NY',
            'timestamp': datetime.now().isoformat(),
            'is_fraud': False
        },
        {
            'transaction_id': 'TEST_002',
            'amount': 5000,
            'merchant_category': 'gambling',
            'location': 'Unknown',
            'timestamp': datetime.now().isoformat(),
            'is_fraud': True
        }
    ]
    
    for txn in test_transactions:
        score = model.predict(txn)
        expected = "FRAUD" if txn['is_fraud'] else "LEGITIMATE"
        prediction = "FRAUD" if score > 0.75 else "LEGITIMATE"
        match = "✓" if prediction == expected else "✗"
        
        print(f"{match} {txn['transaction_id']}: ${txn['amount']} - Score: {score:.2%} - Predicted: {prediction} - Expected: {expected}")
    
    # Save the model
    print("\n" + "="*60)
    model.save('models/fraud_model.pkl')
    
    print("="*60)
    print("✓ Model training complete!")
    print("You can now run the consumer to start detecting fraud.")

if __name__ == '__main__':
    train_and_save_model()