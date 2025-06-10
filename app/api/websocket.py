from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict
import json
from celery.result import AsyncResult
from app.tasks.automation_tasks import process_minpub_task, process_sga_report_task
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
            # Detectar tipo de tarea automáticamente
            task = AsyncResult(task_id)
            if task.state == 'PENDING':
                await manager.send_progress(task_id, {
                    'status': 'pending',
                    'message': 'Tarea en cola'
                })
            elif task.state == 'PROGRESS':
                meta = task.info or {}
                await manager.send_progress(task_id, {
                    'status': 'processing',
                    'message': meta.get('message', 'Procesando...'),
                    'progress': meta.get('progress', 0)
                })
            elif task.state == 'SUCCESS':
                # Si el resultado tiene 'result', es Minpub; si tiene 'file_path', es SGA
                if isinstance(task.result, dict) and 'result' in task.result:
                    await manager.send_progress(task_id, {
                        'status': 'completed',
                        'result': task.result['result']
                    })
                else:
                    await manager.send_progress(task_id, {
                        'status': 'completed'
                    })
                break
            elif task.state == 'FAILURE':
                await manager.send_progress(task_id, {
                    'status': 'failed',
                    'error': str(task.info)
                })
                break
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        manager.disconnect(task_id)
    except Exception as e:
        await manager.send_progress(task_id, {
            'status': 'failed',
            'error': str(e)
        })
        manager.disconnect(task_id) 