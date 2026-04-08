"""Cage management API endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pyairtable import Api
from pydantic import BaseModel

from ..config import settings
from ..helpers.airtable_helpers import (
    generate_cage_preview,
    get_all_cages,
    get_highest_cage_number,
    get_strain_options,
    get_supplier_options,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class AddCagesRequest(BaseModel):
    """Request body for cage preview/creation."""

    mice_per_cage: int
    num_male_cages: int
    num_female_cages: int
    strain: str
    supplier: str
    dob: str
    date_received: str


@router.get("")
def list_cages() -> dict:
    """Get cage statistics and listing."""
    try:
        all_cages = get_all_cages(
            settings.airtable_api_key, settings.airtable_base_id
        )
        total_cages = len(all_cages)
        male_cages = len(
            [c for c in all_cages if c.get("fields", {}).get("sex") == "m"]
        )
        female_cages = len(
            [c for c in all_cages if c.get("fields", {}).get("sex") == "f"]
        )

        return {
            "cage_stats": {
                "total": total_cages,
                "male": male_cages,
                "female": female_cages,
            },
            "cages": all_cages,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/form-options")
def get_cage_form_options() -> dict:
    """Get form options for cage creation."""
    try:
        highest_cage = get_highest_cage_number()
        return {
            "next_cage_num": highest_cage + 1,
            "supplier_options": get_supplier_options(),
            "strain_options": get_strain_options(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/preview")
def preview_add_cages(data: AddCagesRequest) -> dict:
    """Generate a preview of cages to be created."""
    try:
        cage_preview = generate_cage_preview(
            data.mice_per_cage,
            data.num_male_cages,
            data.num_female_cages,
            data.strain,
            data.supplier,
            data.dob,
            data.date_received,
        )

        total_cages = len(cage_preview)
        total_mice = total_cages * data.mice_per_cage
        cage_range = (
            f"{cage_preview[0]['cage_id']} to {cage_preview[-1]['cage_id']}"
            if cage_preview
            else "N/A"
        )

        return {
            "cages": cage_preview,
            "summary": {
                "total_cages": total_cages,
                "male_cages": data.num_male_cages,
                "female_cages": data.num_female_cages,
                "total_mice": total_mice,
                "cage_range": cage_range,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
def create_cages(data: AddCagesRequest) -> dict:
    """Create cages in Airtable."""
    try:
        cage_preview = generate_cage_preview(
            data.mice_per_cage,
            data.num_male_cages,
            data.num_female_cages,
            data.strain,
            data.supplier,
            data.dob,
            data.date_received,
        )

        api = Api(settings.airtable_api_key)
        cages_table = api.table(settings.airtable_base_id, "cages")

        records_to_create = []
        for cage in cage_preview:
            records_to_create.append({
                "cage": cage["cage_id"],
                "n_mice": cage["n_mice"],
                "sex": cage["sex"],
                "strain": cage["strain"],
                "bought_from": [cage["bought_from"]],
                "dob": cage["dob"],
                "received": cage["received"],
                "alive": "True",
            })

        created_records = cages_table.batch_create(records_to_create)

        total_created = len(created_records)
        male_created = len([c for c in cage_preview if c["sex"] == "m"])
        female_created = len([c for c in cage_preview if c["sex"] == "f"])

        return {
            "success": True,
            "stats": {
                "total_created": total_created,
                "male_created": male_created,
                "female_created": female_created,
                "first_cage": (
                    cage_preview[0]["cage_id"] if cage_preview else "N/A"
                ),
                "last_cage": (
                    cage_preview[-1]["cage_id"] if cage_preview else "N/A"
                ),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
