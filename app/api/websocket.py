from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict
import json
from celery.result import AsyncResult
from app.tasks.automation_tasks import process_minpub_task
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, task_id: str):
        await websocket.accept()
        self.active_connections[task_id] = websocket

    def disconnect(self, task_id: str):
        if task_id in self.active_connections:
            del self.active_connections[task_id]

    async def send_progress(self, task_id: str, message: dict):
        if task_id in self.active_connections:
            await self.active_connections[task_id].send_json(message)

manager = ConnectionManager()

async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await manager.connect(websocket, task_id)
    try:
        while True:
            task = process_minpub_task.AsyncResult(task_id)
            
            if task.state == 'PENDING':
                await manager.send_progress(task_id, {
                    'status': 'pending',
                    'message': 'Tarea en cola'
                })
            elif task.state == 'PROGRESS':
                await manager.send_progress(task_id, {
                    'status': 'processing',
                    'message': task.info.get('message', 'Procesando...'),
                    'progress': task.info.get('progress', 0)
                })
            elif task.state == 'SUCCESS':
                await manager.send_progress(task_id, {
                    'status': 'completed',
                    'result': task.result["result"]
                })
                break
            elif task.state == 'FAILURE':
                await manager.send_progress(task_id, {
                    'status': 'failed',
                    'error': str(task.info)
                })
                break
            
            # Wait for 1 second before next update
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        manager.disconnect(task_id)
    except Exception as e:
        await manager.send_progress(task_id, {
            'status': 'failed',
            'error': str(e)
        })
        manager.disconnect(task_id) 