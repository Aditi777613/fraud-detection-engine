from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

router = APIRouter()

# This will be injected from app.py
recent_transactions = []
recent_alerts = []
metrics = {}

def set_data_references(transactions, alerts, metrics_data):
    """Set references to shared data from main app"""
    global recent_transactions, recent_alerts, metrics
    recent_transactions = transactions
    recent_alerts = alerts
    metrics = metrics_data

@router.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    return {
        "status": "healthy",
        "service": "Fraud Detection Engine API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "api": "operational",
            "kafka_consumer": "operational",
            "websocket": "operational"
        }
    }

@router.get("/transactions/recent")
async def get_recent_transactions(
    limit: int = Query(default=50, ge=1, le=100),
    user_id: Optional[str] = None
):
    """Get recent transactions with optional filtering"""
    try:
        filtered_transactions = recent_transactions
        
        # Filter by user_id if provided
        if user_id:
            filtered_transactions = [
                t for t in filtered_transactions 
                if t.get('user_id') == user_id
            ]
        
        # Apply limit
        result = filtered_transactions[-limit:]
        
        return {
            "transactions": result,
            "count": len(result),
            "total_available": len(recent_transactions),
            "filtered": user_id is not None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/transactions/{transaction_id}")
async def get_transaction_by_id(transaction_id: str):
    """Get a specific transaction by ID"""
    try:
        transaction = next(
            (t for t in recent_transactions if t.get('transaction_id') == transaction_id),
            None
        )
        
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        return {
            "transaction": transaction,
            "found": True
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts/recent")
async def get_recent_alerts(
    limit: int = Query(default=50, ge=1, le=100),
    min_score: Optional[float] = Query(default=None, ge=0.0, le=1.0)
):
    """Get recent fraud alerts with optional score filtering"""
    try:
        filtered_alerts = recent_alerts
        
        # Filter by minimum fraud score if provided
        if min_score is not None:
            filtered_alerts = [
                a for a in filtered_alerts 
                if a.get('fraud_score', 0) >= min_score
            ]
        
        # Apply limit
        result = filtered_alerts[-limit:]
        
        return {
            "alerts": result,
            "count": len(result),
            "total_available": len(recent_alerts),
            "filtered": min_score is not None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts/{alert_id}")
async def get_alert_by_id(alert_id: str):
    """Get a specific alert by ID"""
    try:
        alert = next(
            (a for a in recent_alerts if a.get('alert_id') == alert_id),
            None
        )
        
        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        return {
            "alert": alert,
            "found": True
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/summary")
async def get_metrics_summary():
    """Get comprehensive system metrics"""
    try:
        fraud_rate = 0
        avg_transaction_amount = 0
        avg_fraud_amount = 0
        
        if metrics.get('total_transactions', 0) > 0:
            fraud_rate = (metrics.get('total_alerts', 0) / metrics['total_transactions']) * 100
            avg_transaction_amount = metrics.get('total_amount_processed', 0) / metrics['total_transactions']
        
        if metrics.get('total_alerts', 0) > 0:
            avg_fraud_amount = metrics.get('total_amount_flagged', 0) / metrics['total_alerts']
        
        return {
            "totals": {
                "transactions": metrics.get('total_transactions', 0),
                "alerts": metrics.get('total_alerts', 0),
                "amount_processed": round(metrics.get('total_amount_processed', 0), 2),
                "amount_flagged": round(metrics.get('total_amount_flagged', 0), 2)
            },
            "rates": {
                "fraud_rate": round(fraud_rate, 2),
                "avg_transaction_amount": round(avg_transaction_amount, 2),
                "avg_fraud_amount": round(avg_fraud_amount, 2)
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/hourly")
async def get_hourly_analytics():
    """Get hourly breakdown of transactions and alerts"""
    try:
        # Group by hour
        hourly_data = {}
        
        for alert in recent_alerts:
            try:
                timestamp = datetime.fromisoformat(alert['timestamp'])
                hour_key = timestamp.strftime('%Y-%m-%d %H:00')
                
                if hour_key not in hourly_data:
                    hourly_data[hour_key] = {
                        'hour': hour_key,
                        'alerts': 0,
                        'total_amount': 0
                    }
                
                hourly_data[hour_key]['alerts'] += 1
                hourly_data[hour_key]['total_amount'] += alert.get('transaction', {}).get('amount', 0)
            except:
                continue
        
        # Sort by hour
        sorted_data = sorted(hourly_data.values(), key=lambda x: x['hour'])
        
        return {
            "hourly_breakdown": sorted_data,
            "period": {
                "start": sorted_data[0]['hour'] if sorted_data else None,
                "end": sorted_data[-1]['hour'] if sorted_data else None
            },
            "total_hours": len(sorted_data)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/by-category")
async def get_category_analytics():
    """Get fraud analytics by merchant category"""
    try:
        category_stats = {}
        
        for alert in recent_alerts:
            category = alert.get('transaction', {}).get('merchant_category', 'unknown')
            
            if category not in category_stats:
                category_stats[category] = {
                    'category': category,
                    'count': 0,
                    'total_amount': 0,
                    'avg_fraud_score': []
                }
            
            category_stats[category]['count'] += 1
            category_stats[category]['total_amount'] += alert.get('transaction', {}).get('amount', 0)
            category_stats[category]['avg_fraud_score'].append(alert.get('fraud_score', 0))
        
        # Calculate averages
        for category in category_stats.values():
            scores = category['avg_fraud_score']
            category['avg_fraud_score'] = round(sum(scores) / len(scores), 3) if scores else 0
            category['avg_amount'] = round(category['total_amount'] / category['count'], 2)
        
        # Sort by count
        sorted_categories = sorted(
            category_stats.values(), 
            key=lambda x: x['count'], 
            reverse=True
        )
        
        return {
            "categories": sorted_categories,
            "total_categories": len(sorted_categories)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/amount-distribution")
async def get_amount_distribution():
    """Get distribution of fraud alerts by amount ranges"""
    try:
        ranges = {
            '0-100': {'min': 0, 'max': 100, 'count': 0, 'total': 0},
            '100-500': {'min': 100, 'max': 500, 'count': 0, 'total': 0},
            '500-1000': {'min': 500, 'max': 1000, 'count': 0, 'total': 0},
            '1000-5000': {'min': 1000, 'max': 5000, 'count': 0, 'total': 0},
            '5000+': {'min': 5000, 'max': float('inf'), 'count': 0, 'total': 0}
        }
        
        for alert in recent_alerts:
            amount = alert.get('transaction', {}).get('amount', 0)
            
            for range_key, range_data in ranges.items():
                if range_data['min'] <= amount < range_data['max']:
                    range_data['count'] += 1
                    range_data['total'] += amount
                    break
        
        # Format results
        distribution = []
        for range_key, range_data in ranges.items():
            distribution.append({
                'range': range_key,
                'count': range_data['count'],
                'total_amount': round(range_data['total'], 2),
                'avg_amount': round(range_data['total'] / range_data['count'], 2) if range_data['count'] > 0 else 0
            })
        
        return {
            "distribution": distribution,
            "total_alerts": sum(r['count'] for r in distribution)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/trends")
async def get_trends(hours: int = Query(default=24, ge=1, le=168)):
    """Get fraud trends over specified time period"""
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_period_alerts = [
            a for a in recent_alerts
            if datetime.fromisoformat(a['timestamp']) >= cutoff_time
        ]
        
        return {
            "period_hours": hours,
            "alerts_in_period": len(recent_period_alerts),
            "avg_alerts_per_hour": round(len(recent_period_alerts) / hours, 2),
            "total_amount_flagged": round(
                sum(a.get('transaction', {}).get('amount', 0) for a in recent_period_alerts),
                2
            ),
            "trend": "increasing" if len(recent_period_alerts) > len(recent_alerts) / 2 else "stable"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/realtime")
async def get_realtime_stats():
    """Get real-time statistics for dashboard"""
    try:
        last_10_transactions = recent_transactions[-10:] if recent_transactions else []
        last_10_alerts = recent_alerts[-10:] if recent_alerts else []
        
        return {
            "current": {
                "transactions_last_10": len(last_10_transactions),
                "alerts_last_10": len(last_10_alerts),
                "fraud_rate_last_10": round(
                    (len(last_10_alerts) / len(last_10_transactions) * 100)
                    if last_10_transactions else 0,
                    2
                )
            },
            "latest_transaction": last_10_transactions[-1] if last_10_transactions else None,
            "latest_alert": last_10_alerts[-1] if last_10_alerts else None,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))