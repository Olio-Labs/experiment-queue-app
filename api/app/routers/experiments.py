"""Experiment queue API endpoints."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..config import settings
from ..helpers.airtable_helpers import (
    get_all_dropdown_options,
    get_all_experiments_from_queue,
    get_manipulation_details,
)
from ..repositories.airtable_base import AirtableBase
from ..repositories.experiments_repo import ExperimentsRepository
from ..repositories.manipulations_repo import ManipulationsRepository
from ..services.experiment_queue import load_experiment_queue_from_records

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("")
def list_experiments() -> dict:
    """List all active experiments, sorted by priority then start date."""
    try:
        base = AirtableBase.from_env()
        exp_repo = ExperimentsRepository(base)
        manip_repo = ManipulationsRepository(base)
        domain_exps = exp_repo.list_active()

        # Build custom->record_id map for manipulations
        custom_to_recid: dict[str, str] = {}
        for m in manip_repo.list_all_manipulations() or []:
            if "id" in m and "fields" in m and m["fields"].get("manipulation"):
                custom_to_recid[str(m["fields"]["manipulation"])] = m["id"]

        def to_record(e) -> dict:
            unique_manip_ids = [
                custom_to_recid.get(str(mid))
                for mid in (e.manipulation_ids or [])
            ]
            unique_manip_ids = [mid for mid in unique_manip_ids if mid]
            return {
                "id": e.record_id,
                "fields": {
                    "priority": e.priority,
                    "assignment": e.assignment,
                    "num_days": e.num_days,
                    "config_file": e.config_file,
                    "is_chronic": e.is_chronic,
                    "notes": e.notes,
                    "earliest_start_date": (
                        e.earliest_start_date.strftime("%Y-%m-%d")
                        if e.earliest_start_date
                        else None
                    ),
                    "actual_start_date": (
                        e.actual_start_date.strftime("%Y-%m-%d")
                        if e.actual_start_date
                        else None
                    ),
                    "actual_end_date": (
                        e.actual_end_date.strftime("%Y-%m-%d")
                        if e.actual_end_date
                        else None
                    ),
                    "unique_manipulation_ids": unique_manip_ids,
                    "manipulations": ", ".join(
                        [str(m) for m in (e.manipulation_ids or [])]
                    ),
                    "selected_tasks": e.selected_tasks,
                    "experiment_time_minutes": e.experiment_time_minutes,
                },
            }

        records = [to_record(e) for e in domain_exps]

        # Sort using domain model
        dq = load_experiment_queue_from_records(records)
        id_to_exp = {e.record_id: e for e in dq.experiments}

        def sort_key(r: dict):
            exp = id_to_exp.get(r.get("id"))
            if exp is not None:
                try:
                    pr = int(exp.priority)
                except Exception:
                    pr = 9999
                s = exp.earliest_start_date
                return (pr, s is None, s or date.max)
            return (9999, True, date.max)

        records.sort(key=sort_key)

        # Attach manipulation details
        all_manip_ids: set[str] = set()
        for record in records:
            ids = record["fields"].get("unique_manipulation_ids", [])
            if isinstance(ids, list):
                all_manip_ids.update(ids)

        manip_details = get_manipulation_details(
            settings.airtable_api_key,
            settings.airtable_base_id,
            all_manip_ids,
        )

        for record in records:
            manip_ids = record["fields"].get("unique_manipulation_ids", [])
            record["manipulation_details"] = [
                {"id": mid, **manip_details.get(mid, {"drugs": [], "safety": [], "dose_mg_kg": []})}
                for mid in manip_ids
            ]
            record["manipulations_display"] = record["fields"].get(
                "manipulations", ""
            )

        # Collect all field keys as headers
        all_field_keys: set[str] = set()
        for record in records:
            all_field_keys.update(record.get("fields", {}).keys())

        return {
            "experiments": records,
            "headers": sorted(list(all_field_keys)),
        }
    except Exception as e:
        logger.error(f"Error fetching experiments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug")
def debug_domain_queue() -> dict:
    """Debug endpoint to view domain queue status."""
    try:
        from ..services.experiment_queue import load_experiment_queue_from_airtable

        dq = load_experiment_queue_from_airtable()
        type_counts: dict[str, int] = {}
        for exp in dq.experiments:
            t = getattr(exp, "assignment", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        return {
            "status": "ok",
            "num_experiments": len(dq.experiments),
            "assignment_type_counts": type_counts,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/form-options")
def get_form_options() -> dict:
    """Get dropdown options for the experiment form."""
    options = get_all_dropdown_options(
        settings.airtable_api_key,
        settings.airtable_base_id,
        settings.airtable_table_name,
    )
    return {"options": options}


@router.get("/{record_id}")
def get_experiment(record_id: str) -> dict:
    """Get a single experiment by record ID."""
    try:
        from pyairtable import Api

        api = Api(settings.airtable_api_key)
        table = api.table(settings.airtable_base_id, settings.airtable_table_name)
        record = table.get(record_id)
        return {"experiment": record}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("")
def create_experiment(data: dict) -> dict:
    """Create a new experiment in the queue."""
    try:
        from pyairtable import Api

        api = Api(settings.airtable_api_key)
        table = api.table(settings.airtable_base_id, settings.airtable_table_name)
        fields = data.get("fields", data)
        record = table.create(fields)
        return {"success": True, "record": record}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{record_id}")
def update_experiment(record_id: str, data: dict) -> dict:
    """Update an existing experiment."""
    try:
        from pyairtable import Api

        api = Api(settings.airtable_api_key)
        table = api.table(settings.airtable_base_id, settings.airtable_table_name)
        fields = data.get("fields", data)
        record = table.update(record_id, fields)
        return {"success": True, "record": record}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{record_id}")
def delete_experiment(record_id: str) -> dict:
    """Delete an experiment from the queue."""
    try:
        from pyairtable import Api

        api = Api(settings.airtable_api_key)
        table = api.table(settings.airtable_base_id, settings.airtable_table_name)
        table.delete(record_id)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
