"""Scheduling and plan preview API endpoints."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..config import settings
from ..schemas import PushPlanRequest
from ..services.scheduling_orchestrator import SchedulingOrchestrator

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/preview")
def get_plan_preview(
    start_date: Optional[str] = Query(None),
) -> dict:
    """Compute and return the full scheduling preview."""
    parsed_start = None
    if start_date:
        try:
            parsed_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD.",
            )

    try:
        orchestrator = SchedulingOrchestrator(settings)
        result = orchestrator.compute_preview(parsed_start)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error generating plan preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/push")
def push_plan_to_airtable(request: PushPlanRequest) -> dict:
    """Push the scheduled plan to Airtable."""
    try:
        orchestrator = SchedulingOrchestrator(settings)
        result = orchestrator.push_plan(request)
        return result.model_dump()
    except Exception as e:
        logger.error(f"Error pushing plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear")
def clear_scheduled_plan() -> dict:
    """Clear the scheduled plan from Airtable."""
    try:
        orchestrator = SchedulingOrchestrator(settings)
        return orchestrator.clear_plan()
    except Exception as e:
        logger.error(f"Error clearing plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recalculate")
def recalculate_experiment_times() -> dict:
    """Recalculate experiment time estimates."""
    try:
        orchestrator = SchedulingOrchestrator(settings)
        return orchestrator.recalculate_times()
    except Exception as e:
        logger.error(f"Error recalculating times: {e}")
        raise HTTPException(status_code=500, detail=str(e))
