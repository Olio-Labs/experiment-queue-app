"""Airtable helper functions extracted from the monolithic app.py."""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from pyairtable import Api

from ..config import settings

logger = logging.getLogger(__name__)


def get_api() -> Api:
    """Get a configured Airtable API instance."""
    return Api(settings.airtable_api_key)


def get_all_experiments_from_queue(
    api_key: str,
    base_id: str,
    table_name: str,
) -> list[dict]:
    """Fetch all experiment records from the queue table."""
    api = Api(api_key)
    table = api.table(base_id, table_name)
    return table.all()


def get_all_cages(api_key: str, base_id: str) -> list[dict]:
    """Fetch all cage records."""
    api = Api(api_key)
    table = api.table(base_id, "cages")
    return table.all()


def get_all_boxes(api_key: str, base_id: str) -> list[dict]:
    """Fetch all box records."""
    api = Api(api_key)
    table = api.table(base_id, "boxes")
    return table.all()


def get_all_manipulations(api_key: str, base_id: str) -> list[dict]:
    """Fetch all manipulation records."""
    api = Api(api_key)
    table = api.table(base_id, "manipulations")
    return table.all()


def get_all_drugs(api_key: str, base_id: str) -> list[dict]:
    """Fetch all drug records."""
    api = Api(api_key)
    table = api.table(base_id, "drugs")
    return table.all()


def get_all_dropdown_options(
    api_key: str, base_id: str, table_name: str
) -> dict[str, list[str]]:
    """Fetch dropdown/select field options from Airtable metadata API."""
    try:
        import requests

        url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
        headers = {"Authorization": f"Bearer {api_key}"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        options: dict[str, list[str]] = {}
        for table in data.get("tables", []):
            if table.get("name") == table_name:
                for field in table.get("fields", []):
                    if field.get("type") in (
                        "singleSelect",
                        "multipleSelects",
                    ):
                        choices = field.get("options", {}).get("choices", [])
                        options[field["name"]] = [
                            c.get("name", "") for c in choices
                        ]
        return options
    except Exception as e:
        logger.warning(f"Failed to fetch dropdown options: {e}")
        return {}


def get_manipulation_details(
    api_key: str,
    base_id: str,
    manip_ids: set[str],
) -> dict[str, dict]:
    """Fetch manipulation details and resolve drug names."""
    api = Api(api_key)
    manip_table = api.table(base_id, "manipulations")
    drugs_table = api.table(base_id, "drugs")

    if not manip_ids:
        return {}

    # Fetch manipulations
    formula = "OR(" + ",".join(
        [f"RECORD_ID()='{mid}'" for mid in manip_ids]
    ) + ")"
    manip_records = {
        rec["id"]: rec
        for rec in manip_table.all(formula=formula)
        if "fields" in rec
    }

    # Gather drug IDs
    drug_ids: set[str] = set()
    for manip in manip_records.values():
        drugs_field = manip["fields"].get("drugs", [])
        if isinstance(drugs_field, list):
            drug_ids.update(drugs_field)

    # Fetch drugs
    drugs_records: dict[str, dict] = {}
    if drug_ids:
        drug_formula = "OR(" + ",".join(
            [f"RECORD_ID()='{did}'" for did in drug_ids]
        ) + ")"
        drugs_records = {
            rec["id"]: rec
            for rec in drugs_table.all(formula=drug_formula)
            if "fields" in rec
        }

    # Build details
    details: dict[str, dict] = {}
    for manip_id in manip_ids:
        manip = manip_records.get(manip_id, {})
        fields = manip.get("fields", {})
        drug_names = []
        for drug_id in fields.get("drugs", []):
            drug_rec = drugs_records.get(drug_id, {})
            drug_name = drug_rec.get("fields", {}).get("drug", "")
            if drug_name:
                drug_names.append(drug_name)
        details[manip_id] = {
            "drugs": drug_names,
            "safety": fields.get("safety", []),
            "dose_mg_kg": fields.get("dose_mg_kg", []),
        }
    return details


def get_highest_cage_number() -> int:
    """Get the highest cage number currently in the system."""
    api = Api(settings.airtable_api_key)
    table = api.table(settings.airtable_base_id, "cages")
    records = table.all()
    highest = 0
    for rec in records:
        cage_id = rec.get("fields", {}).get("cage", "")
        if cage_id and cage_id.startswith("c"):
            try:
                num = int(cage_id[1:])
                highest = max(highest, num)
            except (ValueError, IndexError):
                pass
    return highest


def get_supplier_options() -> list[str]:
    """Get available supplier options from Airtable."""
    options = get_all_dropdown_options(
        settings.airtable_api_key,
        settings.airtable_base_id,
        "cages",
    )
    return options.get("bought_from", ["Jackson", "Taconic", "Charles River"])


def get_strain_options() -> list[str]:
    """Get available strain options from Airtable."""
    options = get_all_dropdown_options(
        settings.airtable_api_key,
        settings.airtable_base_id,
        "cages",
    )
    return options.get("strain", ["C57BL/6J", "BALB/c"])


def generate_cage_preview(
    mice_per_cage: int,
    num_male_cages: int,
    num_female_cages: int,
    strain: str,
    supplier: str,
    dob: str,
    date_received: str,
) -> list[dict]:
    """Generate a preview of cages to be created."""
    highest = get_highest_cage_number()
    next_num = highest + 1
    preview: list[dict] = []

    for i in range(num_male_cages):
        cage_id = f"c{next_num:07d}"
        preview.append({
            "cage_id": cage_id,
            "n_mice": mice_per_cage,
            "sex": "m",
            "strain": strain,
            "bought_from": supplier,
            "dob": dob,
            "received": date_received,
        })
        next_num += 1

    for i in range(num_female_cages):
        cage_id = f"c{next_num:07d}"
        preview.append({
            "cage_id": cage_id,
            "n_mice": mice_per_cage,
            "sex": "f",
            "strain": strain,
            "bought_from": supplier,
            "dob": dob,
            "received": date_received,
        })
        next_num += 1

    return preview
