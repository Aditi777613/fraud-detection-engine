from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Dict
import json
import asyncio
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages WebSocket connections and broadcasts"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_metadata: Dict[WebSocket, dict] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str = None):
        """Accept and register a new WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # Store metadata
        self.connection_metadata[websocket] = {
            'client_id': client_id or f"client_{len(self.active_connections)}",
            'connected_at': datetime.now().isoformat(),
            'messages_sent': 0
        }
        
        logger.info(f"New WebSocket connection: {self.connection_metadata[websocket]['client_id']}")
        
        # Send welcome message
        await self.send_personal_message({
            'type': 'connection',
            'status': 'connected',
            'message': 'Connected to Fraud Detection Engine',
            'client_id': self.connection_metadata[websocket]['client_id'],
            'timestamp': datetime.now().isoformat()
        }, websocket)
        
        # Notify about active connections
        await self.broadcast({
            'type': 'info',
            'message': f'Active connections: {len(self.active_connections)}',
            'connection_count': len(self.active_connections)
        })
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection"""
        if websocket in self.active_connections:
            client_id = self.connection_metadata.get(websocket, {}).get('client_id', 'unknown')
            self.active_connections.remove(websocket)
            del self.connection_metadata[websocket]
            logger.info(f"WebSocket disconnected: {client_id}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send a message to a specific client"""
        try:
            await websocket.send_json(message)
            if websocket in self.connection_metadata:
                self.connection_metadata[websocket]['messages_sent'] += 1
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict, exclude: WebSocket = None):
        """Broadcast a message to all connected clients"""
        disconnected = []
        
        for connection in self.active_connections:
            if connection == exclude:
                continue
            
            try:
                await connection.send_json(message)
                if connection in self.connection_metadata:
                    self.connection_metadata[connection]['messages_sent'] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)
    
    async def broadcast_transaction(self, transaction: dict):
        """Broadcast a new transaction to all clients"""
        message = {
            'type': 'transaction',
            'data': transaction,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast(message)
    
    async def broadcast_alert(self, alert: dict):
        """Broadcast a fraud alert to all clients"""
        message = {
            'type': 'alert',
            'data': alert,
            'timestamp': datetime.now().isoformat(),
            'priority': 'high' if alert.get('fraud_score', 0) > 0.9 else 'medium'
        }
        await self.broadcast(message)
    
    async def broadcast_metrics(self, metrics: dict):
        """Broadcast updated metrics to all clients"""
        message = {
            'type': 'metrics',
            'data': metrics,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast(message)
    
    async def broadcast_system_status(self, status: dict):
        """Broadcast system status update"""
        message = {
            'type': 'system_status',
            'data': status,
            'timestamp': datetime.now().isoformat()
        }
        await self.broadcast(message)
    
    def get_connection_stats(self):
        """Get statistics about active connections"""
        total_messages = sum(
            meta.get('messages_sent', 0) 
            for meta in self.connection_metadata.values()
        )
        
        return {
            'active_connections': len(self.active_connections),
            'total_messages_sent': total_messages,
            'clients': [
                {
                    'client_id': meta.get('client_id'),
                    'connected_at': meta.get('connected_at'),
                    'messages_sent': meta.get('messages_sent', 0)
                }
                for meta in self.connection_metadata.values()
            ]
        }

# Global connection manager instance
manager = ConnectionManager()

async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint handler"""
    client_id = None
    
    try:
        # Get client ID from query params if provided
        client_id = websocket.query_params.get('client_id')
        
        # Connect the client
        await manager.connect(websocket, client_id)
        
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle different message types
                message_type = message.get('type')
                
                if message_type == 'ping':
                    # Respond to ping
                    await manager.send_personal_message({
                        'type': 'pong',
                        'timestamp': datetime.now().isoformat()
                    }, websocket)
                
                elif message_type == 'subscribe':
                    # Client wants to subscribe to specific events
                    channels = message.get('channels', [])
                    await manager.send_personal_message({
                        'type': 'subscription',
                        'status': 'subscribed',
                        'channels': channels,
                        'timestamp': datetime.now().isoformat()
                    }, websocket)
                
                elif message_type == 'stats_request':
                    # Client requests connection stats
                    stats = manager.get_connection_stats()
                    await manager.send_personal_message({
                        'type': 'stats',
                        'data': stats,
                        'timestamp': datetime.now().isoformat()
                    }, websocket)
                
                else:
                    # Echo back unhandled messages
                    await manager.send_personal_message({
                        'type': 'echo',
                        'original_message': message,
                        'timestamp': datetime.now().isoformat()
                    }, websocket)
            
            except json.JSONDecodeError:
                await manager.send_personal_message({
                    'type': 'error',
                    'message': 'Invalid JSON format',
                    'timestamp': datetime.now().isoformat()
                }, websocket)
            
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await manager.send_personal_message({
                    'type': 'error',
                    'message': str(e),
                    'timestamp': datetime.now().isoformat()
                }, websocket)
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"Client disconnected: {client_id or 'unknown'}")
        
        # Notify other clients
        await manager.broadcast({
            'type': 'info',
            'message': f'Client disconnected. Active connections: {len(manager.active_connections)}',
            'connection_count': len(manager.active_connections)
        })
    
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

async def heartbeat_task():
    """Send periodic heartbeat to all connected clients"""
    while True:
        await asyncio.sleep(30)  # Every 30 seconds
        
        if manager.active_connections:
            await manager.broadcast({
                'type': 'heartbeat',
                'timestamp': datetime.now().isoformat(),
                'connection_count': len(manager.active_connections)
            })

# Export the manager for use in other modules
__all__ = ['manager', 'websocket_endpoint', 'heartbeat_task']