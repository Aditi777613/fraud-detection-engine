from vertexai.preview.generative_models import GenerativeModel
import vertexai
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.gcp_config import GEMINI_API_KEY, GEMINI_MODEL
from src.utils import setup_logger, format_currency

logger = setup_logger('GeminiAnalyzer')

class GeminiAnalyzer:
    """Use Gemini AI for fraud pattern analysis"""
    
    def __init__(self):
        try:
            genai.configure(api_key=GEMINI_API_KEY)
            self.model = genai.GenerativeModel(GEMINI_MODEL)
            logger.info("Gemini AI initialized")
        except Exception as e:
            logger.error(f"Error initializing Gemini: {e}")
            self.model = None
    
    def analyze_transaction(self, transaction, fraud_score):
        """Analyze a suspicious transaction and provide reasoning"""
        if not self.model:
            return self._fallback_analysis(transaction, fraud_score)
        
        try:
            prompt = f"""Analyze this potentially fraudulent transaction:

Transaction Details:
- ID: {transaction['transaction_id']}
- Amount: {format_currency(transaction['amount'])}
- Merchant: {transaction.get('merchant', 'Unknown')}
- Category: {transaction.get('merchant_category', 'Unknown')}
- Location: {transaction.get('location', 'Unknown')}
- Time: {transaction['timestamp']}
- Fraud Score: {fraud_score:.2%}

Provide a brief analysis (2-3 sentences) explaining:
1. Why this transaction might be fraudulent
2. What specific patterns are concerning
3. Recommended action

Keep it concise and actionable."""

            response = self.model.generate_content(prompt)
            analysis = response.text.strip()
            
            logger.info(f"Gemini analysis for {transaction['transaction_id']}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error getting Gemini analysis: {e}")
            return self._fallback_analysis(transaction, fraud_score)
    
    def analyze_patterns(self, transactions, fraud_alerts):
        """Analyze patterns across multiple fraud alerts"""
        if not self.model or len(fraud_alerts) == 0:
            return "Insufficient data for pattern analysis"
        
        try:
            # Summarize the fraud alerts
            summary = f"Analyzing {len(fraud_alerts)} fraud alerts:\n\n"
            for alert in fraud_alerts[:5]:  # Analyze top 5
                summary += f"- {format_currency(alert['amount'])} at {alert.get('merchant', 'Unknown')}\n"
            
            prompt = f"""{summary}

Identify common patterns and trends in these fraudulent transactions. 
What are the key risk indicators? Provide 3-4 bullet points."""

            response = self.model.generate_content(prompt)
            patterns = response.text.strip()
            
            logger.info("Generated pattern analysis")
            return patterns
            
        except Exception as e:
            logger.error(f"Error analyzing patterns: {e}")
            return "Pattern analysis unavailable"
    
    def _fallback_analysis(self, transaction, fraud_score):
        """Simple rule-based analysis when Gemini is unavailable"""
        reasons = []
        
        if transaction['amount'] > 1000:
            reasons.append(f"High transaction amount ({format_currency(transaction['amount'])})")
        
        risky_categories = ['gambling', 'money_transfer', 'cryptocurrency']
        if transaction.get('merchant_category') in risky_categories:
            reasons.append(f"High-risk merchant category: {transaction.get('merchant_category')}")
        
        from datetime import datetime
        timestamp = datetime.fromisoformat(transaction['timestamp'])
        if 2 <= timestamp.hour <= 5:
            reasons.append("Transaction occurred during unusual hours (2 AM - 5 AM)")
        
        if not reasons:
            reasons.append("Multiple risk factors detected by ML model")
        
        analysis = f"FRAUD ALERT: {' | '.join(reasons)}. "
        analysis += f"Fraud confidence: {fraud_score:.1%}. "
        analysis += "Recommended action: Flag for manual review and contact cardholder."
        
        return analysis
    
    def generate_summary(self, daily_stats):
        """Generate a daily summary of fraud detection activities"""
        if not self.model:
            return f"Processed {daily_stats.get('total_transactions', 0)} transactions, detected {daily_stats.get('fraud_alerts', 0)} potential frauds."
        
        try:
            prompt = f"""Generate a brief executive summary for today's fraud detection:

Statistics:
- Total Transactions: {daily_stats.get('total_transactions', 0)}
- Fraud Alerts: {daily_stats.get('fraud_alerts', 0)}
- Total Amount Flagged: {format_currency(daily_stats.get('flagged_amount', 0))}
- Average Fraud Score: {daily_stats.get('avg_fraud_score', 0):.2%}

Provide 2-3 sentences highlighting key findings and recommendations."""

            response = self.model.generate_content(prompt)
            return response.text.strip()
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return f"Processed {daily_stats.get('total_transactions', 0)} transactions, detected {daily_stats.get('fraud_alerts', 0)} potential frauds."

if __name__ == '__main__':
    # Test the analyzer
    from datetime import datetime
    
    analyzer = GeminiAnalyzer()
    
    test_transaction = {
        'transaction_id': 'TEST_001',
        'amount': 5000,
        'merchant': 'Casino Royal',
        'merchant_category': 'gambling',
        'location': 'Las Vegas, NV',
        'timestamp': datetime.now().isoformat()
    }
    
    analysis = analyzer.analyze_transaction(test_transaction, 0.89)
    print(f"\nAnalysis:\n{analysis}")