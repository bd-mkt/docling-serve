import logging

from fastapi import WebSocket

from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_notifier import BaseNotifier
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator

from docling_serve.datamodel.responses import (
    MessageKind,
    TaskStatusResponse,
    WebsocketMessage,
)

_log = logging.getLogger(__name__)


class WebsocketNotifier(BaseNotifier):
    def __init__(self, orchestrator: BaseOrchestrator):
        super().__init__(orchestrator)
        self.task_subscribers: dict[str, set[WebSocket]] = {}

    async def add_task(self, task_id: str):
        self.task_subscribers[task_id] = set()

    async def remove_task(self, task_id: str):
        if task_id in self.task_subscribers:
            for websocket in self.task_subscribers[task_id]:
                await websocket.close()

            del self.task_subscribers[task_id]

    async def _send_to_subscriber(
        self, websocket: WebSocket, message: str
    ) -> bool:
        """Send a message to a single subscriber. Returns False if send failed."""
        try:
            await websocket.send_text(message)
            return True
        except Exception as e:
            _log.warning(f"Failed to send to WebSocket subscriber: {e}")
            return False

    async def notify_task_subscribers(self, task_id: str):
        if task_id not in self.task_subscribers:
            _log.debug(
                f"Task {task_id} has no websocket subscribers, skipping notification."
            )
            return

        try:
            # Get task status from Redis or RQ directly instead of in-memory registry
            task = await self.orchestrator.task_status(task_id=task_id)
            task_queue_position = await self.orchestrator.get_queue_position(task_id)

            msg = TaskStatusResponse(
                task_id=task.task_id,
                task_type=task.task_type,
                task_status=task.task_status,
                task_position=task_queue_position,
                task_meta=task.processing_meta,
                error_detail=task.error_message,
            )
            update_text = WebsocketMessage(
                message=MessageKind.UPDATE, task=msg
            ).model_dump_json()

            failed_subscribers: list[WebSocket] = []
            for websocket in self.task_subscribers[task_id]:
                ok = await self._send_to_subscriber(websocket, update_text)
                if ok and task.is_completed():
                    try:
                        await websocket.close()
                    except Exception:
                        pass
                elif not ok:
                    failed_subscribers.append(websocket)

            # Remove failed subscribers
            for ws in failed_subscribers:
                self.task_subscribers[task_id].discard(ws)

        except Exception as e:
            _log.error(f"Error notifying subscribers for task {task_id}: {e}")
            # Attempt to send error message to all subscribers
            error_text = WebsocketMessage(
                message=MessageKind.ERROR,
                error=f"Failed to retrieve task status: {e}",
            ).model_dump_json()
            for websocket in list(self.task_subscribers.get(task_id, [])):
                await self._send_to_subscriber(websocket, error_text)

    async def notify_queue_positions(self):
        """Notify all subscribers of pending tasks about queue position updates."""
        for task_id in list(self.task_subscribers.keys()):
            try:
                # Check task status directly from Redis or RQ
                task = await self.orchestrator.task_status(task_id)

                # Notify only pending tasks
                if task.task_status == TaskStatus.PENDING:
                    await self.notify_task_subscribers(task_id)
            except Exception as e:
                _log.error(
                    f"Error checking task {task_id} status for queue position notification: {e}"
                )
