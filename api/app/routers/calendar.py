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
def push_to_google_calendar(data: dict) -> dict:
    """Push scheduled experiments to Google Calendar."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        if not settings.google_experiment_calendar_id:
            raise HTTPException(
                status_code=500,
                detail="Google Calendar not configured.",
            )

        service_account_file = settings.google_service_account_file
        if not os.path.exists(service_account_file):
            raise HTTPException(
                status_code=500,
                detail="Service account credentials not found.",
            )

        experiments = data.get("experiments", [])
        if not experiments:
            raise HTTPException(
                status_code=400,
                detail="No experiment data provided.",
            )

        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/calendar"],
        )
        service = build("calendar", "v3", credentials=credentials)
        calendar_id = settings.google_experiment_calendar_id

        events_created = 0
        errors: list[str] = []

        for exp in experiments:
            start = exp.get("scheduled_start_date")
            end = exp.get("scheduled_end_date")
            if not start or not end:
                continue

            title_parts = ["[EQ]"]
            if exp.get("experiment_id"):
                title_parts.append(exp["experiment_id"])
            manips = exp.get("manipulation_ids", [])
            if manips:
                title_parts.append(", ".join(manips[:3]))

            event_body = {
                "summary": " ".join(title_parts),
                "start": {"date": start},
                "end": {"date": end},
                "description": (
                    f"Cages: {', '.join(exp.get('assigned_cages', []))}\n"
                    f"Time: {exp.get('experiment_time_daily', 0)} min/day\n"
                    f"Priority: {exp.get('priority', '')}"
                ),
            }

            try:
                service.events().insert(
                    calendarId=calendar_id,
                    body=event_body,
                ).execute()
                events_created += 1
            except Exception as e:
                errors.append(f"Event for {exp.get('record_id', '?')}: {e}")

        return {
            "success": len(errors) == 0,
            "message": f"{events_created} events created.",
            "events_created": events_created,
            "errors": errors,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
