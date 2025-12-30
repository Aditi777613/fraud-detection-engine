"""
Flink SQL Processor for Real-time Transaction Aggregations

This module demonstrates how you would use Confluent Cloud's Flink SQL
to process streaming transactions in real-time. Flink SQL queries are
typically executed via Confluent Cloud Console or API.

For the hackathon, this file shows the SQL queries you would run.
You can execute these in Confluent Cloud's Flink SQL workspace.
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils import setup_logger

logger = setup_logger('FlinkProcessor')

# Flink SQL Queries for Confluent Cloud
FLINK_SQL_QUERIES = {
    
    "create_transactions_table": """
    -- Create table for transaction stream
    CREATE TABLE transactions (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        merchant STRING,
        merchant_category STRING,
        location STRING,
        timestamp TIMESTAMP(3),
        is_fraud BOOLEAN,
        device_id STRING,
        ip_address STRING,
        WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = 'YOUR_BOOTSTRAP_SERVER',
        'properties.group.id' = 'flink-processor',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    );
    """,
    
    "create_enriched_table": """
    -- Create table for enriched transactions
    CREATE TABLE transactions_enriched (
        transaction_id STRING,
        user_id STRING,
        amount DOUBLE,
        merchant STRING,
        merchant_category STRING,
        location STRING,
        timestamp TIMESTAMP(3),
        is_fraud BOOLEAN,
        device_id STRING,
        ip_address STRING,
        hour_of_day INT,
        day_of_week INT,
        is_high_amount BOOLEAN,
        is_risky_category BOOLEAN
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions-enriched',
        'properties.bootstrap.servers' = 'YOUR_BOOTSTRAP_SERVER',
        'format' = 'json'
    );
    """,
    
    "enrich_transactions": """
    -- Enrich transactions with computed features
    INSERT INTO transactions_enriched
    SELECT 
        transaction_id,
        user_id,
        amount,
        merchant,
        merchant_category,
        location,
        timestamp,
        is_fraud,
        device_id,
        ip_address,
        HOUR(timestamp) AS hour_of_day,
        DAYOFWEEK(timestamp) AS day_of_week,
        CASE WHEN amount > 1000 THEN true ELSE false END AS is_high_amount,
        CASE 
            WHEN merchant_category IN ('gambling', 'cryptocurrency', 'money_transfer') 
            THEN true 
            ELSE false 
        END AS is_risky_category
    FROM transactions;
    """,
    
    "user_transaction_velocity": """
    -- Calculate user transaction velocity (tumbling window)
    CREATE VIEW user_velocity AS
    SELECT 
        user_id,
        TUMBLE_START(timestamp, INTERVAL '1' HOUR) AS window_start,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        MAX(amount) AS max_amount
    FROM transactions
    GROUP BY 
        user_id,
        TUMBLE(timestamp, INTERVAL '1' HOUR);
    """,
    
    "high_value_transactions": """
    -- Filter high-value transactions
    CREATE VIEW high_value_transactions AS
    SELECT 
        transaction_id,
        user_id,
        amount,
        merchant,
        merchant_category,
        location,
        timestamp
    FROM transactions
    WHERE amount > 1000;
    """,
    
    "risky_transactions": """
    -- Detect risky transaction patterns
    CREATE VIEW risky_transactions AS
    SELECT 
        transaction_id,
        user_id,
        amount,
        merchant,
        merchant_category,
        location,
        timestamp,
        CASE 
            WHEN amount > 5000 THEN 'very_high_amount'
            WHEN merchant_category IN ('gambling', 'cryptocurrency') THEN 'risky_category'
            WHEN HOUR(timestamp) BETWEEN 2 AND 5 THEN 'odd_hours'
            ELSE 'other'
        END AS risk_reason
    FROM transactions
    WHERE 
        amount > 1000 
        OR merchant_category IN ('gambling', 'cryptocurrency', 'money_transfer')
        OR HOUR(timestamp) BETWEEN 2 AND 5;
    """,
    
    "transaction_aggregations": """
    -- Real-time aggregations by merchant category
    CREATE VIEW category_stats AS
    SELECT 
        merchant_category,
        TUMBLE_START(timestamp, INTERVAL '5' MINUTE) AS window_start,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        COUNT(DISTINCT user_id) AS unique_users
    FROM transactions
    GROUP BY 
        merchant_category,
        TUMBLE(timestamp, INTERVAL '5' MINUTE);
    """,
    
    "rapid_transactions": """
    -- Detect rapid successive transactions (possible card testing)
    CREATE VIEW rapid_transactions AS
    SELECT 
        user_id,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        HOP_START(timestamp, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start
    FROM transactions
    GROUP BY 
        user_id,
        HOP(timestamp, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
    HAVING COUNT(*) > 5;  -- More than 5 transactions in 5 minutes
    """,
    
    "geographic_anomalies": """
    -- Detect transactions from multiple locations in short time
    CREATE VIEW geographic_anomalies AS
    SELECT 
        user_id,
        COUNT(DISTINCT location) AS location_count,
        COLLECT(location) AS locations,
        TUMBLE_START(timestamp, INTERVAL '1' HOUR) AS window_start
    FROM transactions
    GROUP BY 
        user_id,
        TUMBLE(timestamp, INTERVAL '1' HOUR)
    HAVING COUNT(DISTINCT location) > 2;  -- More than 2 locations in 1 hour
    """
}

class FlinkProcessor:
    """
    Flink SQL Processor for Confluent Cloud
    
    In production, these queries would be executed in Confluent Cloud's
    Flink SQL workspace. This class provides documentation and utilities
    for working with Flink SQL.
    """
    
    def __init__(self):
        logger.info("Flink Processor initialized")
        self.queries = FLINK_SQL_QUERIES
    
    def get_setup_instructions(self):
        """Get instructions for setting up Flink SQL in Confluent Cloud"""
        instructions = """
        ================================================
        CONFLUENT CLOUD FLINK SQL SETUP INSTRUCTIONS
        ================================================
        
        1. Log into Confluent Cloud Console
        2. Navigate to your cluster
        3. Go to "Flink" tab (or "ksqlDB" for older accounts)
        4. Click "Create Compute Pool"
        5. Select region and CFU size (start with 1 CFU for demo)
        
        6. Open Flink SQL Editor
        7. Execute the queries in this order:
           a. create_transactions_table
           b. create_enriched_table
           c. enrich_transactions
           d. user_transaction_velocity
           e. high_value_transactions
           f. risky_transactions
           g. transaction_aggregations
           h. rapid_transactions
           i. geographic_anomalies
        
        8. Monitor running queries in Flink dashboard
        
        Note: Replace 'YOUR_BOOTSTRAP_SERVER' in queries with your
        actual Confluent Cloud bootstrap server.
        ================================================
        """
        return instructions
    
    def print_all_queries(self):
        """Print all Flink SQL queries"""
        print("\n" + "="*60)
        print("FLINK SQL QUERIES FOR CONFLUENT CLOUD")
        print("="*60 + "\n")
        
        for name, query in self.queries.items():
            print(f"\n{'='*60}")
            print(f"Query: {name}")
            print('='*60)
            print(query)
            print()
    
    def get_query(self, query_name: str) -> str:
        """Get a specific Flink SQL query by name"""
        return self.queries.get(query_name, "Query not found")
    
    def export_queries_to_file(self, filepath: str = "flink_queries.sql"):
        """Export all queries to a SQL file"""
        with open(filepath, 'w') as f:
            f.write("-- Flink SQL Queries for Fraud Detection Engine\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write("-- Execute these in Confluent Cloud Flink SQL Workspace\n\n")
            
            for name, query in self.queries.items():
                f.write(f"-- {name}\n")
                f.write("-" * 60 + "\n")
                f.write(query)
                f.write("\n\n")
        
        logger.info(f"Queries exported to {filepath}")
        return filepath

def simulate_flink_aggregation(transactions):
    """
    Simulate Flink-style aggregations locally (for demo purposes)
    In production, this would be handled by Confluent Cloud Flink
    """
    from collections import defaultdict
    
    logger.info("Simulating Flink aggregations locally...")
    
    # Group by merchant category
    category_stats = defaultdict(lambda: {
        'count': 0,
        'total_amount': 0,
        'amounts': []
    })
    
    for txn in transactions:
        category = txn.get('merchant_category', 'unknown')
        category_stats[category]['count'] += 1
        category_stats[category]['total_amount'] += txn.get('amount', 0)
        category_stats[category]['amounts'].append(txn.get('amount', 0))
    
    # Calculate averages
    results = []
    for category, stats in category_stats.items():
        results.append({
            'merchant_category': category,
            'transaction_count': stats['count'],
            'total_amount': round(stats['total_amount'], 2),
            'avg_amount': round(stats['total_amount'] / stats['count'], 2) if stats['count'] > 0 else 0,
            'max_amount': max(stats['amounts']) if stats['amounts'] else 0,
            'min_amount': min(stats['amounts']) if stats['amounts'] else 0
        })
    
    return results

if __name__ == '__main__':
    processor = FlinkProcessor()
    
    print("\n" + "="*60)
    print("FLINK SQL PROCESSOR FOR FRAUD DETECTION ENGINE")
    print("="*60)
    
    # Print setup instructions
    print(processor.get_setup_instructions())
    
    # Option to export queries
    print("\nOptions:")
    print("1. Print all queries")
    print("2. Export queries to file")
    print("3. Show setup instructions")
    
    choice = input("\nEnter your choice (1-3): ")
    
    if choice == '1':
        processor.print_all_queries()
    elif choice == '2':
        filepath = processor.export_queries_to_file()
        print(f"\n✓ Queries exported to: {filepath}")
    elif choice == '3':
        print(processor.get_setup_instructions())
    else:
        print("Invalid choice")
    
    print("\n" + "="*60)
    print("For the hackathon submission:")
    print("- Copy these queries to Confluent Cloud Flink SQL")
    print("- Take screenshots of running queries")
    print("- Show real-time aggregations in your demo")
    print("="*60 + "\n")