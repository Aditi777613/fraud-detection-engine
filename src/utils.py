import logging
import json
import sys
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

# Configure logging
def setup_logger(name, level=logging.INFO):
    """Set up a colored logger"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        
        formatter = logging.Formatter(
            f'{Fore.CYAN}%(asctime)s{Style.RESET_ALL} - '
            f'{Fore.GREEN}%(name)s{Style.RESET_ALL} - '
            f'%(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def serialize_transaction(transaction):
    """Serialize transaction to JSON string"""
    return json.dumps(transaction).encode('utf-8')

def deserialize_transaction(message):
    """Deserialize transaction from Kafka message"""
    try:
        return json.loads(message.value().decode('utf-8'))
    except Exception as e:
        logging.error(f"Error deserializing message: {e}")
        return None

def format_currency(amount):
    """Format amount as currency"""
    return f"${amount:,.2f}"

def calculate_velocity(transactions, time_window_minutes=60):
    """Calculate transaction velocity (transactions per hour)"""
    if not transactions:
        return 0
    
    now = datetime.now()
    recent = [t for t in transactions if (now - t['timestamp']).seconds / 60 <= time_window_minutes]
    
    return len(recent)

def is_suspicious_pattern(transaction):
    """Check for common suspicious patterns"""
    suspicious_indicators = []
    
    # Large transaction
    if transaction['amount'] > 10000:
        suspicious_indicators.append('large_amount')
    
    # Odd hours (2 AM - 5 AM)
    hour = transaction['timestamp'].hour
    if 2 <= hour <= 5:
        suspicious_indicators.append('odd_hours')
    
    # High-risk merchant categories
    risky_categories = ['gambling', 'money_transfer', 'cryptocurrency']
    if transaction.get('merchant_category') in risky_categories:
        suspicious_indicators.append('risky_category')
    
    return suspicious_indicators

def print_fraud_alert(transaction, fraud_score, reason):
    """Print a formatted fraud alert"""
    print(f"\n{Fore.RED}{'='*60}")
    print(f"🚨 FRAUD ALERT DETECTED! 🚨")
    print(f"{'='*60}{Style.RESET_ALL}")
    print(f"Transaction ID: {Fore.YELLOW}{transaction['transaction_id']}{Style.RESET_ALL}")
    print(f"Amount: {Fore.RED}{format_currency(transaction['amount'])}{Style.RESET_ALL}")
    print(f"Fraud Score: {Fore.RED}{fraud_score:.2%}{Style.RESET_ALL}")
    print(f"Merchant: {transaction.get('merchant', 'Unknown')}")
    print(f"Reason: {reason}")
    print(f"{Fore.RED}{'='*60}{Style.RESET_ALL}\n")

def get_timestamp():
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()

def calculate_statistics(values):
    """Calculate basic statistics for a list of values"""
    if not values:
        return {'mean': 0, 'median': 0, 'std': 0}
    
    import numpy as np
    return {
        'mean': float(np.mean(values)),
        'median': float(np.median(values)),
        'std': float(np.std(values))
    }