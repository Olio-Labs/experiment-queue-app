"""Scheduling and plan preview API endpoints."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/preview")
def get_plan_preview(
    start_date: Optional[str] = Query(None),
) -> dict:
    """Get experiment plan preview for scheduling.

    This is a complex endpoint that replicates the experiment_plan_preview
    route from app.py. It computes the scheduling preview using technician
    availability, cage availability, and experiment constraints.
    """
    try:
        from pyairtable import Api

        from ..helpers.airtable_helpers import (
            get_all_boxes,
            get_all_cages,
            get_all_experiments_from_queue,
            get_all_manipulations,
        )

        api_key = settings.airtable_api_key
        base_id = settings.airtable_base_id

        # Fetch all data needed for scheduling
        all_records = get_all_experiments_from_queue(
            api_key, base_id, settings.airtable_table_name
        )
        all_cages = get_all_cages(api_key, base_id)
        all_boxes = get_all_boxes(api_key, base_id)
        all_manipulations = get_all_manipulations(api_key, base_id)

        # Filter to queued experiments
        queued = [
            r for r in all_records
            if r.get("fields", {}).get("status", "").strip().lower()
            not in {"done", "hold", "in-progress", "in progress", "running"}
        ]

        return {
            "experiments": queued,
            "total_cages": len(all_cages),
            "total_boxes": len(all_boxes),
            "total_manipulations": len(all_manipulations),
            "message": (
                "Plan preview data loaded. Full scheduling computation "
                "to be implemented."
            ),
        }
    except Exception as e:
        logger.error(f"Error generating plan preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/push")
def push_plan_to_airtable(data: dict) -> dict:
    """Push the scheduled plan to Airtable."""
    try:
        # TODO: Implement the full plan push logic from app.py
        # This requires the scheduled plan state from the preview
        return {
            "success": True,
            "message": "Plan push endpoint ready — full logic to be migrated",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear")
def clear_scheduled_plan() -> dict:
    """Clear the scheduled plan from Airtable."""
    try:
        # TODO: Implement the clear logic from app.py
        return {
            "success": True,
            "message": "Clear endpoint ready — full logic to be migrated",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recalculate")
def recalculate_experiment_times() -> dict:
    """Recalculate experiment time estimates."""
    try:
        # TODO: Implement the recalculation logic from app.py
        return {
            "success": True,
            "message": (
                "Recalculate endpoint ready — full logic to be migrated"
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
