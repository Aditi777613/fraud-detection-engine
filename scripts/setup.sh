#!/bin/bash

echo "======================================"
echo "Fraud Detection Engine - Setup Script"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Python 3 found${NC}"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${YELLOW}! Virtual environment already exists${NC}"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
echo -e "${GREEN}✓ Pip upgraded${NC}"

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"

# Create necessary directories
echo ""
echo "Creating directories..."
mkdir -p models
mkdir -p logs
mkdir -p data
echo -e "${GREEN}✓ Directories created${NC}"

# Check for .env file
echo ""
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}! No .env file found${NC}"
    echo "Copying .env.example to .env..."
    cp .env.example .env
    echo -e "${YELLOW}! Please edit .env file with your credentials before running the application${NC}"
else
    echo -e "${GREEN}✓ .env file exists${NC}"
fi

# Validate configuration
echo ""
echo "Validating configuration..."
python -c "from config.confluent_config import validate_config; from config.gcp_config import validate_config as validate_gcp; validate_config(); validate_gcp()" 2>&1

# Train initial model
echo ""
echo "Would you like to train the fraud detection model now? (y/n)"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Training model..."
    python scripts/train_model.py
    echo -e "${GREEN}✓ Model trained${NC}"
else
    echo -e "${YELLOW}! Skipping model training${NC}"
    echo "  You can train the model later with: python scripts/train_model.py"
fi

# Setup complete
echo ""
echo "======================================"
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo "======================================"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your credentials (if not done already)"
echo "2. Start the producer:    python src/producer.py"
echo "3. Start the consumer:    python src/consumer.py"
echo "4. Start the backend:     cd backend && uvicorn app:app --reload"
echo "5. Open frontend:         open frontend/index.html"
echo ""
echo "Or run demo traffic:      python scripts/demo_traffic.py"
echo ""
echo "======================================"