"""Calendar API endpoints."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException

from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/weekly")
def get_weekly_calendar() -> dict:
    """Get calendar embed URL for weekly view."""
    tech_calendar_id = settings.google_tech_calendar_id
    experiment_calendar_id = settings.google_experiment_calendar_id

    calendar_url = (
        f"https://calendar.google.com/calendar/embed?"
        f"height=600&wkst=1&ctz=America%2FLos_Angeles&mode=WEEK&"
        f"showTabs=0&showPrint=0&showCalendars=1&showTitle=0&"
        f"src={tech_calendar_id}&"
        f"src={experiment_calendar_id}&"
        f"color=%23F4511E&"
        f"color=%2333B679"
    )

    return {"calendar_url": calendar_url}


@router.post("/push")
def push_to_google_calendar() -> dict:
    """Push scheduled experiments to Google Calendar."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        service_account_file = settings.google_service_account_file
        if not os.path.exists(service_account_file):
            raise HTTPException(
                status_code=500,
                detail="Google service account credentials not found",
            )

        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/calendar"],
        )
        service = build("calendar", "v3", credentials=credentials)

        # TODO: Implement the actual push logic from app.py
        # This requires the scheduling preview state which needs to be
        # passed from the frontend or computed here

        return {"success": True, "message": "Calendar push not yet implemented"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
