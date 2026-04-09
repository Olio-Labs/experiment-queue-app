"""Airtable helper functions extracted from the monolithic app.py."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from pyairtable import Api

from ..config import settings

logger = logging.getLogger(__name__)


def get_api() -> Api:
    """Get a configured Airtable API instance."""
    return Api(settings.airtable_api_key)


def get_all_records(api_key: str, base_id: str, table_name: str) -> list[dict]:
    """Generic helper to fetch all records from a table."""
    if not all([api_key, base_id, table_name]):
        logger.warning(f"Missing Airtable config for fetching {table_name}.")
        return []
    try:
        table = Api(api_key).table(base_id, table_name)
        return table.all()
    except Exception as e:
        logger.error(f"Error fetching all records from {table_name}: {e}")
        return []


def get_all_experiments_from_queue(
    api_key: str,
    base_id: str,
    table_name: str,
) -> list[dict]:
    """Fetch all experiments, excluding 'done' and 'hold' status."""
    if not all([api_key, base_id, table_name]):
        logger.warning(
            f"Missing Airtable config for fetching experiments from {table_name}."
        )
        return []
    try:
        table = Api(api_key).table(base_id, table_name)
        formula = "AND({status}!='done',{status}!='hold')"
        return table.all(formula=formula)
    except Exception as e:
        logger.error(f"Error fetching experiments from {table_name}: {e}")
        return []


def get_in_progress_experiments_from_queue(
    api_key: str, base_id: str, table_name: str
) -> list[dict]:
    """Fetch experiments with status 'in_progress'."""
    if not all([api_key, base_id, table_name]):
        return []
    try:
        table = Api(api_key).table(base_id, table_name)
        formula = "{status}='in_progress'"
        return table.all(formula=formula)
    except Exception as e:
        logger.error(f"Error fetching in-progress experiments: {e}")
        return []


def get_scheduled_experiments_from_queue(
    api_key: str, base_id: str, table_name: str
) -> list[dict]:
    """Fetch experiments with status 'scheduled'."""
    if not all([api_key, base_id, table_name]):
        return []
    try:
        table = Api(api_key).table(base_id, table_name)
        formula = "{status}='scheduled'"
        return table.all(formula=formula)
    except Exception as e:
        logger.error(f"Error fetching scheduled experiments: {e}")
        return []


def get_all_cages(api_key: str, base_id: str) -> list[dict]:
    """Fetch all alive cage records."""
    formula = "{alive}='True'"
    try:
        table = Api(api_key).table(base_id, "cages")
        return table.all(formula=formula)
    except Exception as e:
        logger.error(f"Error fetching filtered cages: {e}")
        return []


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
                        options[field["name"]] = [c.get("name", "") for c in choices]
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
    formula = "OR(" + ",".join([f"RECORD_ID()='{mid}'" for mid in manip_ids]) + ")"
    manip_records = {
        rec["id"]: rec for rec in manip_table.all(formula=formula) if "fields" in rec
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
        drug_formula = (
            "OR(" + ",".join([f"RECORD_ID()='{did}'" for did in drug_ids]) + ")"
        )
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
        preview.append(
            {
                "cage_id": cage_id,
                "n_mice": mice_per_cage,
                "sex": "m",
                "strain": strain,
                "bought_from": supplier,
                "dob": dob,
                "received": date_received,
            }
        )
        next_num += 1

    for i in range(num_female_cages):
        cage_id = f"c{next_num:07d}"
        preview.append(
            {
                "cage_id": cage_id,
                "n_mice": mice_per_cage,
                "sex": "f",
                "strain": strain,
                "bought_from": supplier,
                "dob": dob,
                "received": date_received,
            }
        )
        next_num += 1

    return preview


# --- Scheduling data-fetch helpers ---


def get_all_manipulations_details(api_key: str, base_id: str) -> dict[str, dict]:
    """Fetch all manipulation records as a dict keyed by record ID."""
    records = get_all_records(api_key, base_id, "manipulations")
    return {record["id"]: record for record in records if "id" in record}


def get_all_drugs_details(api_key: str, base_id: str) -> dict[str, dict]:
    """Fetch all drug records as a dict keyed by record ID."""
    records = get_all_records(api_key, base_id, "drugs")
    return {record["id"]: record for record in records if "id" in record}


def get_all_drug_inventory(api_key: str, base_id: str) -> list[dict]:
    """Fetch all drug inventory records."""
    return get_all_records(api_key, base_id, "drug_inventory")


def get_task_times_dict(api_key: str, base_id: str) -> dict[str, float]:
    """Fetch the task_times table as a dict mapping task name to minutes."""
    try:
        records = get_all_records(api_key, base_id, "task_times")
        task_time_dict: dict[str, float] = {}
        if not records:
            logger.warning("No records found in 'task_times' table.")
            return task_time_dict

        for rec in records:
            fields = rec.get("fields", {})
            task = fields.get("task")
            minutes = fields.get("minutes")
            if task and isinstance(minutes, (int, float)):
                task_time_dict[str(task)] = float(minutes)
        return task_time_dict
    except Exception as e:
        logger.error(f"Error fetching task_times: {e}")
        return {}


def parse_airtable_date_for_scheduling(
    date_str: Optional[str],
    format_str: str = "%Y-%m-%d",
) -> Optional[date]:
    """Parse YYYY-MM-DD date strings from Airtable."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, format_str).date()
    except ValueError:
        logger.warning(
            f"Could not parse date string '{date_str}' with format '{format_str}'."
        )
        return None


def get_potential_cage_pool_from_airtable(
    api_key: str, base_id: str
) -> list[dict[str, str]]:
    """Fetch cages from boxes b0000009-b0000088 for pseudorandom assignment.

    Filters out boxes with non-empty status and cages that are not alive.
    Returns a list of cage dicts with airtable_record_id, custom_cage_id,
    sex, and full_fields.
    """
    boxes_table_name = settings.boxes_table_name
    cages_table_name = settings.cages_table_name

    if not all([api_key, base_id, boxes_table_name, cages_table_name]):
        logger.warning(
            "Missing Airtable config for get_potential_cage_pool_from_airtable."
        )
        return []

    current_api = Api(api_key)
    box_table = current_api.table(base_id, boxes_table_name)
    cage_table = current_api.table(base_id, cages_table_name)

    potential_cages_list: list[dict] = []
    excluded_cages_by_status: dict[str, str] = {}

    target_box_custom_ids = [f"b{str(i).zfill(7)}" for i in range(9, 89)]
    if not target_box_custom_ids:
        return []

    formula_parts = [f"{{box_id}}='{bid}'" for bid in target_box_custom_ids]
    box_filter_formula = "OR(" + ",".join(formula_parts) + ")"

    try:
        boxes_formula = f"AND({box_filter_formula}, NOT({{use_type}}='testing'))"
        box_records = box_table.all(formula=boxes_formula)

        linked_cage_record_ids: list[str] = []
        for box_rec in box_records:
            if "fields" not in box_rec or "cages" not in box_rec["fields"]:
                continue
            box_status = box_rec["fields"].get("status", "")
            linked_ids = box_rec["fields"]["cages"]
            if box_status:
                if isinstance(linked_ids, list) and len(linked_ids) > 0:
                    excluded_cages_by_status[linked_ids[0]] = box_status
                continue
            if isinstance(linked_ids, list) and len(linked_ids) > 0:
                linked_cage_record_ids.append(linked_ids[0])

        if not linked_cage_record_ids:
            logger.info("No linked cage records found from boxes.")
            return []

        cage_id_parts = [f"RECORD_ID()='{rid}'" for rid in linked_cage_record_ids]
        cage_filter = "OR(" + ",".join(cage_id_parts) + ")"
        cages_final_filter = f"AND({cage_filter}, {{alive}}='True')"
        cage_records = cage_table.all(formula=cages_final_filter)

        for cage_rec in cage_records:
            fields = cage_rec.get("fields", {})
            airtable_id = cage_rec.get("id")
            custom_id = fields.get("cage")
            animal_s = fields.get("sex")
            if all([airtable_id, custom_id, animal_s]):
                potential_cages_list.append(
                    {
                        "airtable_record_id": airtable_id,
                        "custom_cage_id": str(custom_id),
                        "sex": str(animal_s).lower(),
                        "full_fields": fields,
                    }
                )

        if excluded_cages_by_status:
            logger.info(
                f"Excluded {len(excluded_cages_by_status)} cages "
                f"due to non-empty box status."
            )

    except Exception as e:
        logger.error(f"Error in get_potential_cage_pool_from_airtable: {e}")
        return []

    return potential_cages_list


def get_experiment_planner_history_from_airtable(
    api_key: str,
    base_id: str,
    relevant_custom_cage_ids: list[str],
) -> list[dict]:
    """Fetch historical experiment assignments from the planner table.

    Returns a list of dicts with cage_id, manipulation_id, start_date,
    and experiment_series.
    """
    planner_table_name = settings.experiment_planner_copy_testing_table_name
    if not all([api_key, base_id, planner_table_name]):
        logger.warning("Missing Airtable config for planner history.")
        return []
    if not relevant_custom_cage_ids:
        return []

    planner_table = Api(api_key).table(base_id, planner_table_name)
    history_data: list[dict] = []

    cage_id_field = "cage_id"
    manip_id_field = "manipulation_id"
    start_date_field = "start_date"

    formula_parts = [f"{{{cage_id_field}}}='{cid}'" for cid in relevant_custom_cage_ids]
    filter_formula = "OR(" + ",".join(formula_parts) + ")"
    fields_to_retrieve = [
        cage_id_field,
        manip_id_field,
        start_date_field,
        "experiment_series",
    ]

    try:
        planner_records = planner_table.all(
            formula=filter_formula, fields=fields_to_retrieve
        )
        for rec in planner_records:
            fields = rec.get("fields", {})
            custom_cage_id = fields.get(cage_id_field)
            manip_id = fields.get(manip_id_field)
            start_date_str = fields.get(start_date_field)
            experiment_series = fields.get("experiment_series", "")

            if all([custom_cage_id, manip_id, start_date_str]):
                parsed_date = parse_airtable_date_for_scheduling(start_date_str)
                if parsed_date:
                    history_data.append(
                        {
                            "cage_id": str(custom_cage_id),
                            "manipulation_id": str(manip_id),
                            "start_date": parsed_date,
                            "experiment_series": (
                                str(experiment_series) if experiment_series else ""
                            ),
                        }
                    )
    except Exception as e:
        logger.error(f"Error in get_experiment_planner_history_from_airtable: {e}")
        return []

    return history_data


def get_vehicle_drug_and_manip_maps(
    api_key: str, base_id: str
) -> tuple[dict[str, list[str]], set[str]]:
    """Identify vehicle drugs and their linked manipulations.

    Returns:
        Tuple of (vehicle_drug_to_manip_record_ids_map,
        all_vehicle_manip_record_ids).
    """
    vehicle_map: dict[str, list[str]] = {}
    all_vehicle_manip_ids: set[str] = set()

    if not api_key or not base_id:
        return vehicle_map, all_vehicle_manip_ids

    try:
        drugs_table = Api(api_key).table(base_id, "drugs")
        all_drug_records = drugs_table.all(fields=["drug_type", "manipulations"])

        for drug_rec in all_drug_records:
            drug_id = drug_rec.get("id")
            fields = drug_rec.get("fields", {})
            drug_types = fields.get("drug_type", [])
            linked_manip_ids = fields.get("manipulations", [])

            if not drug_id:
                continue

            is_vehicle = isinstance(drug_types, list) and "vehicle" in drug_types

            if is_vehicle:
                valid_ids = (
                    [m_id for m_id in linked_manip_ids if isinstance(m_id, str)]
                    if linked_manip_ids
                    else []
                )
                if valid_ids:
                    vehicle_map[drug_id] = valid_ids
                    all_vehicle_manip_ids.update(valid_ids)

        logger.info(
            f"Found {len(vehicle_map)} vehicle drugs mapping "
            f"to {len(all_vehicle_manip_ids)} manipulation IDs."
        )

    except Exception as e:
        logger.error(f"Error in get_vehicle_drug_and_manip_maps: {e}")
        return {}, set()

    return vehicle_map, all_vehicle_manip_ids


def get_existing_syringe_color_assignments_from_planner(
    api_key: str, base_id: str
) -> dict[date, set[str]]:
    """Fetch existing syringe color assignments from planner table."""
    planner_table_name = settings.experiment_planner_copy_testing_table_name
    result: dict[date, set[str]] = {}
    try:
        planner_table = Api(api_key).table(base_id, planner_table_name)
        records = planner_table.all(fields=["start_date", "syringe_color"])
        for rec in records:
            fields = rec.get("fields", {})
            date_str = fields.get("start_date")
            color = fields.get("syringe_color")
            if date_str and color:
                parsed = parse_airtable_date_for_scheduling(date_str)
                if parsed:
                    if parsed not in result:
                        result[parsed] = set()
                    result[parsed].add(str(color))
    except Exception as e:
        logger.error(f"Error fetching syringe color assignments: {e}")
    return result


def get_table_schema_from_metadata(
    api_key: str, base_id: str, table_name_to_find: str
) -> Optional[dict]:
    """Fetch table schema from Airtable Metadata API."""
    import requests

    if not all([api_key, base_id, table_name_to_find]):
        logger.warning("Missing parameters for get_table_schema_from_metadata.")
        return None

    meta_api_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        response = requests.get(meta_api_url, headers=headers, timeout=15)
        response.raise_for_status()
        meta_data = response.json()

        for t_schema in meta_data.get("tables", []):
            if t_schema.get("name") == table_name_to_find:
                return t_schema
        logger.warning(f"Table '{table_name_to_find}' not found in metadata.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Airtable metadata for schema: {e}")
        return None


def extract_options_from_field_schema(
    field_schema: Optional[dict],
    field_name: str,
    table_name: str,
) -> Optional[list[str]]:
    """Extract select options from a field schema."""
    if not field_schema:
        logger.warning(
            f"No schema provided for field '{field_name}' in table '{table_name}'."
        )
        return None

    if field_schema.get("type") in ("singleSelect", "multipleSelects"):
        choices = field_schema.get("options", {}).get("choices", [])
        return [choice.get("name") for choice in choices if choice.get("name")]
    else:
        logger.warning(
            f"Field '{field_name}' in table '{table_name}' is not a select type."
        )
        return None
