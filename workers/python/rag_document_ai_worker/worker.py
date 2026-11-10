"""Temporal Python SDK Worker polling doc-ai-task-queue."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import Optional

from temporalio.client import Client
from temporalio.worker import Worker

from .activity import DispatchDocAIExtractActivity

DOC_AI_TASK_QUEUE = "doc-ai-task-queue"

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
)
logger = logging.getLogger("rag_document_ai_worker")


async def create_worker(
    temporal_host: str = "localhost:7233",
    namespace: str = "default",
    task_queue: str = DOC_AI_TASK_QUEUE,
    client: Optional[Client] = None,
) -> Worker:
    """Connects to Temporal cluster and configures worker polling task_queue."""
    if client is None:
        client = await Client.connect(temporal_host, namespace=namespace)

    worker = Worker(
        client,
        task_queue=task_queue,
        activities=[DispatchDocAIExtractActivity],
    )
    return worker


async def run_worker() -> None:
    """Main worker event loop with graceful shutdown signal handling."""
    host = os.environ.get("TEMPORAL_HOST_PORT", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", DOC_AI_TASK_QUEUE)

    logger.info(
        "Connecting to Temporal cluster at %s (namespace: %s, queue: %s)",
        host,
        namespace,
        task_queue,
    )

    client = await Client.connect(host, namespace=namespace)
    worker = await create_worker(client=client, task_queue=task_queue)

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Received termination signal, initiating graceful worker drain...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Signal handling on Windows
            signal.signal(sig, lambda *_: _signal_handler())

    logger.info("Worker polling %s started successfully", task_queue)
    async with worker:
        await stop_event.wait()
    logger.info("Worker stopped successfully")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
        sys.exit(0)

