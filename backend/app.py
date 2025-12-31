from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from routes import router, set_data_references
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer
import json
import asyncio
from datetime import datetime
from typing import List
from collections import defaultdict
import sys
import os

# Add parent to path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

try:
    from config.confluent_config import CONSUMER_CONFIG, TOPIC_TRANSACTIONS, TOPIC_ALERTS
    KAFKA_ENABLED = True
except:
    KAFKA_ENABLED = False
    CONSUMER_CONFIG = {}
    TOPIC_TRANSACTIONS = "transactions"
    TOPIC_ALERTS = "fraud-alerts"

app = FastAPI(title="Fraud Detection Engine API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage
recent_transactions: List[dict] = []
recent_alerts: List[dict] = []
metrics = {
    'total_transactions': 0,
    'total_alerts': 0,
    'total_amount_processed': 0.0,
    'total_amount_flagged': 0.0,
}
active_connections: List[WebSocket] = []

@app.get("/")
async def root():
    return {
        "status": "running",
        "service": "Fraud Detection Engine",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/transactions")
async def get_transactions(limit: int = 50):
    return {
        "transactions": recent_transactions[-limit:],
        "count": len(recent_transactions)
    }

@app.get("/alerts")
async def get_alerts(limit: int = 50):
    return {
        "alerts": recent_alerts[-limit:],
        "count": len(recent_alerts)
    }

@app.get("/metrics")
async def get_metrics():
    fraud_rate = 0
    if metrics['total_transactions'] > 0:
        fraud_rate = (metrics['total_alerts'] / metrics['total_transactions']) * 100
    
    return {
        "total_transactions": metrics['total_transactions'],
        "total_alerts": metrics['total_alerts'],
        "fraud_rate": round(fraud_rate, 2),
        "total_amount_processed": round(metrics['total_amount_processed'], 2),
        "total_amount_flagged": round(metrics['total_amount_flagged'], 2),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/analytics")
async def get_analytics():
    hourly_data = defaultdict(int)
    for alert in recent_alerts:
        try:
            timestamp = datetime.fromisoformat(alert['timestamp'])
            hour_key = timestamp.strftime('%Y-%m-%d %H:00')
            hourly_data[hour_key] += 1
        except:
            pass
    
    amount_ranges = {'0-100': 0, '100-500': 0, '500-1000': 0, '1000-5000': 0, '5000+': 0}
    for alert in recent_alerts:
        amount = alert.get('transaction', {}).get('amount', 0)
        if amount < 100:
            amount_ranges['0-100'] += 1
        elif amount < 500:
            amount_ranges['100-500'] += 1
        elif amount < 1000:
            amount_ranges['500-1000'] += 1
        elif amount < 5000:
            amount_ranges['1000-5000'] += 1
        else:
            amount_ranges['5000+'] += 1
    
    return {
        "hourly_alerts": [{"hour": k, "count": v} for k, v in sorted(hourly_data.items())],
        "amount_distribution": [{"range": k, "count": v} for k, v in amount_ranges.items()],
        "recent_trend": len(recent_alerts[-10:])
    }

@app.get("/analytics/hourly")
async def get_hourly_analytics():
    """Get hourly breakdown - THIS IS THE ROUTE FOR HOURLY BUTTON"""
    hourly_data = defaultdict(int)
    
    for alert in recent_alerts:
        try:
            timestamp = datetime.fromisoformat(alert['timestamp'])
            hour_key = timestamp.strftime('%Y-%m-%d %H:00')
            hourly_data[hour_key] += 1
        except:
            continue
    
    sorted_data = sorted([
        {"hour": k, "count": v} 
        for k, v in hourly_data.items()
    ], key=lambda x: x['hour'])
    
    return {
        "hourly_alerts": sorted_data[-24:],
        "period": "hourly",
        "total_hours": len(sorted_data),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/analytics/daily")
async def get_daily_analytics():
    """Get daily breakdown - THIS IS THE ROUTE FOR DAILY BUTTON"""
    daily_data = defaultdict(int)
    
    for alert in recent_alerts:
        try:
            timestamp = datetime.fromisoformat(alert['timestamp'])
            day_key = timestamp.strftime('%Y-%m-%d')
            daily_data[day_key] += 1
        except:
            continue
    
    sorted_data = sorted([
        {"day": k, "count": v} 
        for k, v in daily_data.items()
    ], key=lambda x: x['day'])
    
    return {
        "daily_alerts": sorted_data[-30:],
        "period": "daily",
        "total_days": len(sorted_data),
        "timestamp": datetime.now().isoformat()
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def broadcast_update(message: dict):
    for connection in active_connections:
        try:
            await connection.send_json(message)
        except:
            pass

async def kafka_consumer_task():
    if not KAFKA_ENABLED:
        return
    
    consumer = Consumer({**CONSUMER_CONFIG, 'group.id': 'api-consumer-group'})
    consumer.subscribe([TOPIC_TRANSACTIONS, TOPIC_ALERTS])
    
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        if msg.error():
            continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            
            if msg.topic() == TOPIC_TRANSACTIONS:
                recent_transactions.append(data)
                if len(recent_transactions) > 100:
                    recent_transactions.pop(0)
                metrics['total_transactions'] += 1
                metrics['total_amount_processed'] += data.get('amount', 0)
                await broadcast_update({'type': 'transaction', 'data': data})
            
            elif msg.topic() == TOPIC_ALERTS:
                recent_alerts.append(data)
                if len(recent_alerts) > 100:
                    recent_alerts.pop(0)
                metrics['total_alerts'] += 1
                metrics['total_amount_flagged'] += data.get('transaction', {}).get('amount', 0)
                await broadcast_update({'type': 'alert', 'data': data})
        except Exception as e:
            print(f"Error: {e}")
        
        await asyncio.sleep(0.01)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_consumer_task())
    print("🚀 Fraud Detection Engine API started on http://0.0.0.0:8000")
    print("📚 API Docs: http://0.0.0.0:8000/docs")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
