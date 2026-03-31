"""FastAPI server for receiving manual signals from clear-api."""

import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.tasks.process import process_manual_signal

logger = logging.getLogger(__name__)

app = FastAPI(title="CLEAR Pipeline API", version="1.0.0")


class ManualSignalRequest(BaseModel):
    signal_id: str
    source_type: str  # "field_officer", "partner", "government"
    title: str
    description: str
    severity: int | None = None
    user_id: str


@app.post("/api/manual-signal")
async def receive_manual_signal(req: ManualSignalRequest):
    """
    Receive a manually created signal from clear-api.
    Queues it for event grouping and auto-escalation via Celery.
    """
    logger.info(
        "Received manual signal: id=%s source_type=%s user=%s",
        req.signal_id,
        req.source_type,
        req.user_id,
    )

    # Queue the Celery task
    process_manual_signal.delay(
        signal_id=req.signal_id,
        source_type=req.source_type,
        title=req.title,
        description=req.description,
        severity=req.severity,
        user_id=req.user_id,
    )

    return {"status": "queued", "signal_id": req.signal_id}


@app.get("/health")
async def health():
    return {"status": "ok"}
