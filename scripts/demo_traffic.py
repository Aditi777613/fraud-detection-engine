import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.producer import TransactionProducer
import time
import argparse

def run_demo(duration=60, transactions_per_second=2, fraud_rate=0.2):
    """
    Run a demo transaction stream
    
    Args:
        duration: Duration in seconds (0 for infinite)
        transactions_per_second: Number of transactions per second
        fraud_rate: Probability of generating fraudulent transactions (0.0 to 1.0)
    """
    print("="*60)
    print("🚀 FRAUD DETECTION ENGINE - DEMO MODE")
    print("="*60)
    print(f"Configuration:")
    print(f"  • Duration: {'Infinite' if duration == 0 else f'{duration} seconds'}")
    print(f"  • Transaction Rate: {transactions_per_second} per second")
    print(f"  • Fraud Rate: {fraud_rate*100:.0f}%")
    print("="*60)
    print("\nStarting transaction stream...")
    print("Press Ctrl+C to stop\n")
    
    producer = TransactionProducer()
    interval = 1.0 / transactions_per_second
    
    try:
        if duration == 0:
            # Run indefinitely
            producer.run(interval=interval, fraud_probability=fraud_rate)
        else:
            # Run for specified duration
            start_time = time.time()
            count = 0
            
            while time.time() - start_time < duration:
                transaction = producer.generator.generate_transaction(fraud_rate)
                producer.produce_transaction(transaction)
                count += 1
                
                if count % 10 == 0:
                    elapsed = time.time() - start_time
                    remaining = duration - elapsed
                    print(f"  [{elapsed:.0f}s / {duration}s] Generated {count} transactions ({remaining:.0f}s remaining)")
                
                time.sleep(interval)
            
            producer.producer.flush()
            print(f"\n✓ Demo complete! Generated {count} transactions in {duration} seconds")
    
    except KeyboardInterrupt:
        print("\n\n✓ Demo stopped by user")
        producer.producer.flush()

def main():
    parser = argparse.ArgumentParser(description='Generate demo transaction traffic')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds (0 for infinite)')
    parser.add_argument('--rate', type=float, default=2.0, help='Transactions per second')
    parser.add_argument('--fraud-rate', type=float, default=0.2, help='Fraud probability (0.0 to 1.0)')
    
    args = parser.parse_args()
    
    # Validate fraud rate
    if not 0.0 <= args.fraud_rate <= 1.0:
        print("Error: fraud-rate must be between 0.0 and 1.0")
        sys.exit(1)
    
    run_demo(
        duration=args.duration,
        transactions_per_second=args.rate,
        fraud_rate=args.fraud_rate
    )

if __name__ == '__main__':
    main()