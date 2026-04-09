from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from .notes_parser import parse_notes

logger = logging.getLogger(__name__)


def estimate_time_direct_mapping_from_notes(
    notes: str,
    task_times: Dict[str, float],
    all_cages_data: List[Dict],
) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse map=... from notes and compute 1-day experiment time.
    Matches legacy calculate_experiment_time_cage_mapping behavior.
    """
    if not notes or not isinstance(notes, str):
        return None, "Notes field is required for cage mapping."

    parsed_map, _ = parse_notes(notes)
    if not isinstance(parsed_map, dict) or not parsed_map:
        return (
            None,
            "Notes must contain a valid 'map={...}'"
            " for cage mapping. Parsed data invalid.",
        )

    custom_cage_ids_in_map = set()
    for cage_list_or_item in parsed_map.values():
        if isinstance(cage_list_or_item, list):
            custom_cage_ids_in_map.update(
                str(c) for c in cage_list_or_item if isinstance(c, (str, int))
            )
        elif isinstance(cage_list_or_item, (str, int)):
            custom_cage_ids_in_map.add(str(cage_list_or_item))

    num_total_cages = len(custom_cage_ids_in_map)
    if num_total_cages == 0:
        return None, "Map must result in at least one unique cage."
    if not task_times:
        return 0.0, None

    manipulation_ids_in_map = list(parsed_map.keys())
    if len(manipulation_ids_in_map) == 1 and manipulation_ids_in_map[0] == "m0000004":
        # m0000004: special-case mice-based calculation
        all_cages_by_custom_id = {
            cage.get("fields", {}).get("cage"): cage for cage in all_cages_data
        }
        total_mice_in_experiment = 0
        for custom_id in custom_cage_ids_in_map:
            cage_record = all_cages_by_custom_id.get(custom_id)
            if cage_record:
                cage_fields = cage_record.get("fields", {})
                mice_count = cage_fields.get("n_mice", 0)
                if isinstance(mice_count, (int, float)):
                    total_mice_in_experiment += int(mice_count)
        health_check_time = task_times.get("health_check", 0)
        mouse_weight_time = task_times.get("mouse_weight", 0)
        total_time = total_mice_in_experiment * (health_check_time + mouse_weight_time)
        msg = (
            f"m0000004 experiment:"
            f" {total_mice_in_experiment} mice"
            f" * ({health_check_time} + {mouse_weight_time})"
            f" = {total_time}"
        )
        logger.info(msg)
        return round(total_time, 2), None

    # Count box vs innovive cages
    BOX_LINK_FIELD_NAME = "box"
    all_cages_by_custom_id = {
        cage.get("fields", {}).get("cage"): cage for cage in all_cages_data
    }

    num_box_cages = 0
    num_innovive_cages = 0
    for custom_id in custom_cage_ids_in_map:
        cage_record = all_cages_by_custom_id.get(custom_id)
        if cage_record and cage_record.get("fields", {}).get(BOX_LINK_FIELD_NAME):
            num_box_cages += 1
        else:
            num_innovive_cages += 1

    # Count mice
    total_mice_in_experiment = 0
    for custom_id in custom_cage_ids_in_map:
        cage_record = all_cages_by_custom_id.get(custom_id)
        if cage_record:
            cage_fields = cage_record.get("fields", {})
            mice_count = cage_fields.get("n_mice", 0)
            if isinstance(mice_count, (int, float)):
                total_mice_in_experiment += int(mice_count)

    water_fill_time = task_times.get("water_fill", 0)
    injection_time = task_times.get("injection", 0)
    wheel_clean_time = task_times.get("wheel_clean", 0)
    hopper_fill_time = task_times.get("hopper_fill", 0)
    mouse_weight_time = task_times.get("mouse_weight", 0)
    hopper_fill_innovive_time = task_times.get("hopper_fill_innovive", 0)
    mouse_weight_innovive_time = task_times.get("mouse_weight_innovive", 0)

    total_time = 0.0
    total_time += num_total_cages * water_fill_time
    total_time += total_mice_in_experiment * injection_time
    num_wheel_cleans = num_total_cages // 3 if num_total_cages > 0 else 0
    total_time += num_wheel_cleans * wheel_clean_time
    total_time += num_box_cages * hopper_fill_time
    total_time += num_box_cages * mouse_weight_time
    total_time += num_innovive_cages * hopper_fill_innovive_time
    total_time += num_innovive_cages * mouse_weight_innovive_time

    return round(total_time, 2), None


def estimate_time_pseudorandom(
    cages_per_manip_general: int,
    num_manipulations: int,
    task_times: Dict[str, float],
    all_cages_data: List[Dict],
    manip_custom_ids_list: Optional[List[str]] = None,
    all_manipulations_map: Optional[Dict[str, dict]] = None,
    all_drugs_map: Optional[Dict[str, dict]] = None,
    manip_name_to_record_id_map: Optional[Dict[str, str]] = None,
    cages_per_vehicle: int = 4,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Compute 1-day experiment time for pseudorandom assignment.
    Mirrors legacy calculate_experiment_time_pseudorandom.
    """
    error_parts = []
    if not isinstance(cages_per_manip_general, int) or cages_per_manip_general < 0:
        error_parts.append("Cages per manip must be a non-negative integer.")
    if not isinstance(num_manipulations, int) or num_manipulations < 0:
        error_parts.append("Number of manipulations must be a non-negative integer.")
    if error_parts:
        return None, " ".join(error_parts)
    if not task_times:
        return None, "Task times data is missing, cannot calculate experiment time."

    # m0000004 special case
    if (
        manip_custom_ids_list
        and len(manip_custom_ids_list) == 1
        and manip_custom_ids_list[0] == "m0000004"
    ):
        estimated_mice_per_cage = 2
        num_mice_in_experiment = int(cages_per_manip_general * estimated_mice_per_cage)
        health_check_time = task_times.get("health_check", 0)
        mouse_weight_time = task_times.get("mouse_weight", 0)
        total_time = num_mice_in_experiment * (health_check_time + mouse_weight_time)
        msg = (
            f"m0000004 pseudorandom:"
            f" {num_mice_in_experiment} mice"
            f" * ({health_check_time} + {mouse_weight_time})"
            f" = {total_time}"
        )
        logger.info(msg)
        return round(total_time, 2), None

    total_num_cages = 0
    if (
        manip_custom_ids_list
        and all_manipulations_map
        and all_drugs_map
        and manip_name_to_record_id_map
    ):
        for manip_custom_id in manip_custom_ids_list:
            manip_record_id = manip_name_to_record_id_map.get(manip_custom_id)
            if not manip_record_id:
                total_num_cages += cages_per_manip_general
                continue
            manip_detail = all_manipulations_map.get(manip_record_id)
            if (
                not manip_detail
                or not isinstance(manip_detail, dict)
                or "fields" not in manip_detail
            ):
                total_num_cages += cages_per_manip_general
                continue
            manip_fields = manip_detail.get("fields", {})
            if not isinstance(manip_fields, dict):
                total_num_cages += cages_per_manip_general
                continue
            drug_record_ids = manip_fields.get("drugs", [])
            if not isinstance(drug_record_ids, list):
                drug_record_ids = []
            is_vehicle_manip = False
            for drug_record_id in drug_record_ids:
                drug_detail = all_drugs_map.get(drug_record_id)
                if (
                    drug_detail
                    and isinstance(drug_detail, dict)
                    and "fields" in drug_detail
                ):
                    drug_fields = drug_detail.get("fields", {})
                    if isinstance(drug_fields, dict):
                        drug_types = drug_fields.get("drug_type", [])
                        if isinstance(drug_types, list) and "vehicle" in drug_types:
                            is_vehicle_manip = True
                            break
            if is_vehicle_manip:
                total_num_cages += cages_per_vehicle
            else:
                total_num_cages += cages_per_manip_general
    else:
        total_num_cages = cages_per_manip_general * num_manipulations

    if total_num_cages == 0:
        return 0.0, None

    num_box_cages = total_num_cages
    hopper_fill_time = task_times.get("hopper_fill", 0)
    water_fill_time = task_times.get("water_fill", 0)
    mouse_weight_time = task_times.get("mouse_weight", 0)
    injection_time = task_times.get("injection", 0)
    wheel_clean_time = task_times.get("wheel_clean", 0)

    experiment_time_per_day = 0.0
    experiment_time_per_day += num_box_cages * hopper_fill_time
    experiment_time_per_day += total_num_cages * water_fill_time
    experiment_time_per_day += num_box_cages * mouse_weight_time
    estimated_mice_per_cage = 2
    num_mice_in_experiment = int(total_num_cages * estimated_mice_per_cage)
    experiment_time_per_day += num_mice_in_experiment * injection_time
    if total_num_cages > 0:
        num_wheel_cleans = total_num_cages // 3
        experiment_time_per_day += num_wheel_cleans * wheel_clean_time

    return round(experiment_time_per_day, 2), None


def estimate_time_from_tasks(
    selected_tasks: List[str],
    task_times: Dict[str, float],
    all_cages_data: List[Dict],
    assignment_type: str,
    notes: str,
    exp_fields: Dict[str, object] | None = None,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Task-based time estimation unified for both assignment types.
    Mirrors legacy calculate_experiment_time_based_on_tasks.
    """
    if not selected_tasks:
        return None, "No tasks selected for experiment."
    if not task_times:
        return None, "Task times data is missing, cannot calculate experiment time."

    total_time = 0.0
    exp_fields = exp_fields or {}

    if assignment_type == "direct_mapping" and notes:
        parsed_map, _ = parse_notes(notes)
        if not isinstance(parsed_map, dict) or not parsed_map:
            return None, "Notes must contain a valid 'map={...}' for cage mapping."

        custom_cage_ids_in_map = set()
        for cage_list_or_item in parsed_map.values():
            if isinstance(cage_list_or_item, list):
                custom_cage_ids_in_map.update(
                    str(c) for c in cage_list_or_item if isinstance(c, (str, int))
                )
            elif isinstance(cage_list_or_item, (str, int)):
                custom_cage_ids_in_map.add(str(cage_list_or_item))

        num_total_cages = len(custom_cage_ids_in_map)
        if num_total_cages == 0:
            return None, "Map must result in at least one unique cage."

        all_cages_by_custom_id = {
            cage.get("fields", {}).get("cage"): cage for cage in all_cages_data
        }
        total_mice_in_experiment = 0
        num_box_cages = 0
        num_innovive_cages = 0
        BOX_LINK_FIELD_NAME = "box"

        for custom_id in custom_cage_ids_in_map:
            cage_record = all_cages_by_custom_id.get(custom_id)
            if cage_record:
                cage_fields = cage_record.get("fields", {})
                mice_count = cage_fields.get("n_mice", 0)
                if isinstance(mice_count, (int, float)):
                    total_mice_in_experiment += int(mice_count)
                if cage_fields.get(BOX_LINK_FIELD_NAME):
                    num_box_cages += 1
                else:
                    num_innovive_cages += 1

    elif assignment_type == "pseudorandom":
        cages_per_manip_val = exp_fields.get("cages_per_manip", 8)
        try:
            cages_per_manip_val = int(cages_per_manip_val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            cages_per_manip_val = 8
        _, parsed_manips = parse_notes(notes) if notes else ({}, [])
        num_manipulations = len(parsed_manips) if isinstance(parsed_manips, list) else 0
        num_total_cages = int(cages_per_manip_val) * num_manipulations
        num_box_cages = num_total_cages
        num_innovive_cages = 0
        estimated_mice_per_cage = 2
        total_mice_in_experiment = int(num_total_cages * estimated_mice_per_cage)
    else:
        return (
            None,
            f"Unknown assignment type '{assignment_type}' for task-based calculation.",
        )

    for task in selected_tasks:
        task_time = task_times.get(task, 0)
        if task == "mri":
            mri_setup = task_times.get("mri_setup", 0)
            mri_cleanup = task_times.get("mri_cleanup", 0)
            mri_collect = task_times.get("mri_collect", 0)
            task_time = (
                mri_setup + mri_cleanup + (mri_collect * total_mice_in_experiment)
            )
        elif task == "wheel_clean":
            num_wheel_cleans = num_total_cages // 3 if num_total_cages > 0 else 0
            task_time = num_wheel_cleans * task_time
        elif task in ("hopper_weight", "water_weight", "mouse_weight"):
            task_time = num_box_cages * task_time
        elif task == "mouse_weight_innovive":
            task_time = num_innovive_cages * task_time
        total_time += task_time

    return round(total_time, 2), None
