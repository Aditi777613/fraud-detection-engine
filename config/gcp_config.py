import os
from dotenv import load_dotenv

load_dotenv()

# Google Cloud Project Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
VERTEX_AI_LOCATION = os.getenv('VERTEX_AI_LOCATION', 'us-central1')

# Gemini Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = 'models/gemini-2.5-flash-lite'

# Vertex AI Model Configuration
MODEL_NAME = 'fraud-detection-model'
MODEL_VERSION = 'v1'

# Application Settings
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', '0.25'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))

def validate_config():
    """Validate that all required GCP configuration is present"""
    required_vars = [
        'GCP_PROJECT_ID',
        'GEMINI_API_KEY'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    print("✓ GCP configuration validated")

if __name__ == '__main__':
    validate_config()