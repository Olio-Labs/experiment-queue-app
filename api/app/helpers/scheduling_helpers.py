"""Scheduling helper functions extracted from the monolithic app.py.

Contains cage assignment, syringe color management, experiment splitting,
and resource pre-commitment logic.
"""

from __future__ import annotations

import copy
import logging
import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import settings
from ..services.notes_parser import parse_notes

logger = logging.getLogger(__name__)

# Constants for Pseudorandom Assignment Logic
USE_EXACT_TWO_BOXES_FOR_SPECIAL = True
AIRTABLE_DATE_FORMAT_STR = "%Y-%m-%d"
AIRTABLE_CAGE_MANIPULATION_HISTORY_FIELD_NAME = (
    "manipulation_history"
)
WASHOUT_MANIPULATION_STRING = "washout"


# --- Thin wrappers delegating to service layer ---


def parse_notes_for_scheduling(
    notes_str: Optional[str],
) -> tuple[Optional[dict], list]:
    """Parse notes string to extract direct mapping and manipulation IDs."""
    direct_map, manips_list = parse_notes(notes_str)
    return direct_map, manips_list


def calculate_experiment_time_cage_mapping(
    notes: str,
    task_times: dict[str, float],
    all_cages_data: list[dict],
) -> tuple[Optional[float], Optional[str]]:
    """Calculate experiment time for direct mapping assignment."""
    from ..services.time_estimation import (
        estimate_time_direct_mapping_from_notes,
    )

    return estimate_time_direct_mapping_from_notes(
        notes, task_times, all_cages_data
    )


def calculate_experiment_time_pseudorandom(
    cages_per_manip_general: int,
    num_manipulations: int,
    task_times: dict[str, float],
    all_cages_data: list[dict],
    manip_custom_ids_list: Optional[list[str]] = None,
    all_manipulations_map: Optional[dict[str, dict]] = None,
    all_drugs_map: Optional[dict[str, dict]] = None,
    manip_name_to_record_id_map: Optional[dict[str, str]] = None,
    cages_per_vehicle: int = 4,
) -> tuple[Optional[float], Optional[str]]:
    """Calculate experiment time for pseudorandom assignment."""
    from ..services.time_estimation import (
        estimate_time_pseudorandom,
    )

    return estimate_time_pseudorandom(
        cages_per_manip_general,
        num_manipulations,
        task_times,
        all_cages_data,
        manip_custom_ids_list,
        all_manipulations_map,
        all_drugs_map,
        manip_name_to_record_id_map,
        cages_per_vehicle,
    )


def select_cages_spatially_with_availability(
    manip_id_to_assign: str,
    num_cages_to_select: int,
    candidate_cages: list[dict[str, str]],
    planner_history: list[dict],
    current_scheduling_date: date,
    num_days_duration: int,
    preview_booked_cages: dict[str, set[date]],
    cages_booked_within_current_experiment_run: set[str],
    is_cage_available_func: Any,
    parse_airtable_date_func: Any,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    live_all_cages_details: list[dict],
    effective_last_use_tracker: dict[str, date],
    cage_to_box_group_map: dict[str, int],
    used_box_groups: set,
) -> list[str]:
    """Select cages with spatial distribution and availability."""
    from ..services.cage_availability_service import (
        select_cages_spatially_with_availability as _svc,
    )

    return _svc(
        manip_id_to_assign,
        num_cages_to_select,
        candidate_cages,
        planner_history,
        current_scheduling_date,
        num_days_duration,
        preview_booked_cages,
        cages_booked_within_current_experiment_run,
        is_cage_available_func,
        parse_airtable_date_func,
        airtable_date_format_str,
        cage_manip_history_field_name,
        washout_manip_str,
        live_all_cages_details,
        effective_last_use_tracker,
        cage_to_box_group_map,
        used_box_groups,
    )


# --- Mice and box counting helpers ---


def calculate_total_mice_for_experiment(
    assigned_cage_ids: list[str],
    all_cages_data: list[dict],
) -> int:
    """Calculate total mice for an experiment based on assigned cages."""
    total_mice = 0
    if not assigned_cage_ids:
        return 0
    for cage_record in all_cages_data:
        if cage_record.get("id") in assigned_cage_ids:
            mice_count = cage_record.get("fields", {}).get(
                "n_mice", 0
            )
            if isinstance(mice_count, (int, float)):
                total_mice += int(mice_count)
    return total_mice


def calculate_unique_boxes_for_experiment(
    assigned_cage_ids: list[str],
    all_cages_data: list[dict],
    box_record_id_to_box_id_map: dict[str, str],
) -> set:
    """Calculate unique box IDs for assigned cages."""
    unique_boxes: set[str] = set()
    if not assigned_cage_ids:
        return unique_boxes
    for cage_record in all_cages_data:
        if cage_record.get("id") in assigned_cage_ids:
            box_links = cage_record.get("fields", {}).get("box", [])
            if box_links and isinstance(box_links, list):
                for box_record_id in box_links:
                    box_id = box_record_id_to_box_id_map.get(
                        box_record_id, "unknown"
                    )
                    if box_id not in ("b0000000", "unknown"):
                        unique_boxes.add(box_id)
    return unique_boxes


def calculate_nonbox_cages_for_experiment(
    assigned_cage_ids: list[str],
    all_cages_data: list[dict],
) -> int:
    """Count non-box cages (those without a box assignment)."""
    nonbox_count = 0
    for cage_id in assigned_cage_ids:
        cage_record = next(
            (c for c in all_cages_data if c["id"] == cage_id),
            None,
        )
        if not cage_record:
            continue
        box_links = cage_record.get("fields", {}).get("box", [])
        if (
            not box_links
            or not isinstance(box_links, list)
            or len(box_links) == 0
        ):
            nonbox_count += 1
    return nonbox_count


# --- Core scheduling functions ---


def assign_cages_pseudorandomly_py(
    experiment_record: dict,
    potential_cages_pool: list[dict[str, str]],
    planner_history: list[dict],
    current_scheduling_date: date,
    num_days_duration: int,
    preview_booked_cages: dict[str, set[date]],
    is_cage_available_func: Any,
    parse_airtable_date_func: Any,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    live_all_cages_details: list[dict],
    manips_to_assign_custom_ids: list[str],
    vehicle_manips_for_special_handling_custom_ids: set[str],
    effective_last_use_tracker: dict[str, date],
    cage_to_box_group_map: dict[str, int],
) -> tuple[Optional[dict[str, list[str]]], Optional[str]]:
    """Assign cages pseudorandomly based on sex, recency, availability.

    Returns (assignment_map, error_message).
    assignment_map keys are custom_manip_id, values are lists of
    cage airtable_record_ids.
    """
    exp_fields = experiment_record.get("fields", {})
    cages_per_general_manip_str = exp_fields.get("cages_per_manip")

    if not manips_to_assign_custom_ids:
        return None, (
            "No manipulations provided to "
            "assign_cages_pseudorandomly_py."
        )

    try:
        cages_per_general_manip = (
            int(cages_per_general_manip_str)
            if cages_per_general_manip_str
            else 0
        )
        if cages_per_general_manip < 0:
            raise ValueError(
                "'cages_per_manip' must be non-negative."
            )
    except ValueError as e:
        return None, (
            f"Invalid 'cages_per_manip' for general manips: {e}"
        )

    logger.info(
        f"Pseudo assign: ExpID {experiment_record.get('id')}, "
        f"manips={manips_to_assign_custom_ids}, "
        f"vehicle={vehicle_manips_for_special_handling_custom_ids}, "
        f"cages_per_general={cages_per_general_manip}"
    )

    current_available_male = [
        c for c in potential_cages_pool if c["sex"] == "m"
    ]
    current_available_female = [
        c for c in potential_cages_pool if c["sex"] == "f"
    ]

    master_assignment_map: dict[str, list[str]] = {}
    cages_booked_this_run: set[str] = set()
    male_used_groups: set = set()
    female_used_groups: set = set()

    for manip_custom_id in manips_to_assign_custom_ids:
        selected_cages: list[str] = []
        is_vehicle = (
            manip_custom_id
            in vehicle_manips_for_special_handling_custom_ids
        )

        if is_vehicle:
            cages_per_vehicle_str = exp_fields.get(
                "cages_per_vehicle"
            )
            try:
                cages_per_vehicle = (
                    int(cages_per_vehicle_str)
                    if cages_per_vehicle_str
                    else 4
                )
                if cages_per_vehicle < 0:
                    raise ValueError(
                        "'cages_per_vehicle' must be non-negative."
                    )
            except ValueError as e:
                return None, (
                    f"Invalid 'cages_per_vehicle': {e}"
                )

            sex_assignment = exp_fields.get(
                "sex_assignment", "evenly_split"
            )
            if sex_assignment == "male_only":
                num_male = cages_per_vehicle
                num_female = 0
            elif sex_assignment == "female_only":
                num_male = 0
                num_female = cages_per_vehicle
            else:
                num_male = cages_per_vehicle // 2
                num_female = cages_per_vehicle - num_male

            num_needed = num_male + num_female

            temp_male = select_cages_spatially_with_availability(
                manip_custom_id,
                num_male,
                current_available_male,
                planner_history,
                current_scheduling_date,
                num_days_duration,
                preview_booked_cages,
                cages_booked_this_run,
                is_cage_available_func,
                parse_airtable_date_func,
                airtable_date_format_str,
                cage_manip_history_field_name,
                washout_manip_str,
                live_all_cages_details,
                effective_last_use_tracker,
                cage_to_box_group_map,
                male_used_groups,
            )
            selected_cages.extend(temp_male)

            temp_female: list[str] = []
            if len(temp_male) == num_male:
                temp_female = (
                    select_cages_spatially_with_availability(
                        manip_custom_id,
                        num_female,
                        current_available_female,
                        planner_history,
                        current_scheduling_date,
                        num_days_duration,
                        preview_booked_cages,
                        cages_booked_this_run,
                        is_cage_available_func,
                        parse_airtable_date_func,
                        airtable_date_format_str,
                        cage_manip_history_field_name,
                        washout_manip_str,
                        live_all_cages_details,
                        effective_last_use_tracker,
                        cage_to_box_group_map,
                        female_used_groups,
                    )
                )
                selected_cages.extend(temp_female)

            if len(selected_cages) == num_needed:
                master_assignment_map[manip_custom_id] = (
                    selected_cages
                )
                for cid in selected_cages:
                    cages_booked_this_run.add(cid)
                current_available_male = [
                    c
                    for c in current_available_male
                    if c["airtable_record_id"] not in temp_male
                ]
                current_available_female = [
                    c
                    for c in current_available_female
                    if c["airtable_record_id"] not in temp_female
                ]
            else:
                return None, (
                    f"Could not assign enough cages for "
                    f"vehicle manip '{manip_custom_id}'. "
                    f"Required: {num_needed}, "
                    f"Got: {len(selected_cages)}"
                )

        else:
            num_needed = cages_per_general_manip
            if num_needed == 0:
                master_assignment_map[manip_custom_id] = []
                continue

            sex_assignment = exp_fields.get(
                "sex_assignment", "evenly_split"
            )
            if sex_assignment == "male_only":
                target_male = num_needed
                target_female = 0
            elif sex_assignment == "female_only":
                target_male = 0
                target_female = num_needed
            else:
                target_male = num_needed // 2
                target_female = num_needed - target_male

            selected_males: list[str] = []
            if target_male > 0:
                selected_males = (
                    select_cages_spatially_with_availability(
                        manip_custom_id,
                        target_male,
                        current_available_male,
                        planner_history,
                        current_scheduling_date,
                        num_days_duration,
                        preview_booked_cages,
                        cages_booked_this_run,
                        is_cage_available_func,
                        parse_airtable_date_func,
                        airtable_date_format_str,
                        cage_manip_history_field_name,
                        washout_manip_str,
                        live_all_cages_details,
                        effective_last_use_tracker,
                        cage_to_box_group_map,
                        male_used_groups,
                    )
                )
            selected_cages.extend(selected_males)
            for cid in selected_males:
                cages_booked_this_run.add(cid)
            current_available_male = [
                c
                for c in current_available_male
                if c["airtable_record_id"] not in selected_males
            ]

            selected_females: list[str] = []
            if target_female > 0:
                selected_females = (
                    select_cages_spatially_with_availability(
                        manip_custom_id,
                        target_female,
                        current_available_female,
                        planner_history,
                        current_scheduling_date,
                        num_days_duration,
                        preview_booked_cages,
                        cages_booked_this_run,
                        is_cage_available_func,
                        parse_airtable_date_func,
                        airtable_date_format_str,
                        cage_manip_history_field_name,
                        washout_manip_str,
                        live_all_cages_details,
                        effective_last_use_tracker,
                        cage_to_box_group_map,
                        female_used_groups,
                    )
                )
            selected_cages.extend(selected_females)
            for cid in selected_females:
                cages_booked_this_run.add(cid)
            current_available_female = [
                c
                for c in current_available_female
                if c["airtable_record_id"] not in selected_females
            ]

            if len(selected_cages) == num_needed:
                master_assignment_map[manip_custom_id] = (
                    selected_cages
                )
            else:
                return None, (
                    f"Could not assign enough cages for "
                    f"general manip '{manip_custom_id}'. "
                    f"Required: {num_needed}, "
                    f"Got: {len(selected_cages)}"
                )

    if len(master_assignment_map) == len(
        manips_to_assign_custom_ids
    ):
        logger.info(
            f"Successfully generated assignment map: "
            f"{master_assignment_map}"
        )
        return master_assignment_map, None
    else:
        missing = [
            m
            for m in manips_to_assign_custom_ids
            if m not in master_assignment_map
        ]
        return None, (
            f"Failed to assign all manipulations. "
            f"Expected {len(manips_to_assign_custom_ids)}, "
            f"map has {len(master_assignment_map)}. "
            f"Missing: {missing}"
        )


# --- Syringe color management ---


def check_and_reserve_syringe_colors_for_experiment_duration(
    exp_id_for_log: str,
    manip_custom_ids_needed: set[str],
    proposed_start_date: date,
    num_days_duration: int,
    daily_used_syringe_colors: dict[date, set[str]],
    daily_color_owner_map: dict[date, dict[str, str]],
    manip_to_persistent_color_map: dict[str, str],
    available_syringe_colors_master_list: list[str],
) -> tuple[bool, Optional[dict[str, str]], Optional[str]]:
    """Check if unique syringe colors can be assigned for an experiment.

    Returns (can_schedule, tentative_assignments, error_message).
    """
    tentative: dict[str, str] = {}
    temp_colors: dict[date, set[str]] = defaultdict(set)
    temp_owners: dict[date, dict[str, str]] = defaultdict(dict)

    if not manip_custom_ids_needed:
        return True, {}, None

    if not available_syringe_colors_master_list:
        return False, None, (
            "No syringe colors available in master list."
        )

    colors_to_try = list(available_syringe_colors_master_list)
    random.shuffle(colors_to_try)

    for manip_id in sorted(list(manip_custom_ids_needed)):
        color_assigned = False
        assigned_color: Optional[str] = None

        # 1. Try persistent color
        if manip_id in manip_to_persistent_color_map:
            persistent_color = manip_to_persistent_color_map[
                manip_id
            ]
            can_use = True
            for day_offset in range(num_days_duration):
                check_date = proposed_start_date + timedelta(
                    days=day_offset
                )
                global_owner = daily_color_owner_map.get(
                    check_date, {}
                ).get(persistent_color)
                temp_owner = temp_owners.get(
                    check_date, {}
                ).get(persistent_color)
                used_globally = persistent_color in (
                    daily_used_syringe_colors.get(
                        check_date, set()
                    )
                )
                used_temp = persistent_color in temp_colors.get(
                    check_date, set()
                )

                if used_globally and global_owner not in (
                    manip_id,
                    None,
                ):
                    can_use = False
                    break
                if used_temp and temp_owner not in (
                    manip_id,
                    None,
                ):
                    can_use = False
                    break
            if can_use:
                assigned_color = persistent_color
                color_assigned = True

        # 2. Try new color
        if not color_assigned:
            for candidate in colors_to_try:
                can_use = True
                for day_offset in range(num_days_duration):
                    check_date = proposed_start_date + timedelta(
                        days=day_offset
                    )
                    global_owner = daily_color_owner_map.get(
                        check_date, {}
                    ).get(candidate)
                    temp_owner = temp_owners.get(
                        check_date, {}
                    ).get(candidate)
                    used_globally = candidate in (
                        daily_used_syringe_colors.get(
                            check_date, set()
                        )
                    )
                    used_temp = candidate in temp_colors.get(
                        check_date, set()
                    )

                    if used_globally and global_owner not in (
                        manip_id,
                        None,
                    ):
                        can_use = False
                        break
                    if used_temp and temp_owner not in (
                        manip_id,
                        None,
                    ):
                        can_use = False
                        break
                if can_use:
                    assigned_color = candidate
                    color_assigned = True
                    break

        if not color_assigned or assigned_color is None:
            return False, None, (
                f"[Exp {exp_id_for_log}] No available syringe "
                f"color for manip '{manip_id}' starting "
                f"{proposed_start_date}."
            )

        tentative[manip_id] = assigned_color
        for day_offset in range(num_days_duration):
            check_date = proposed_start_date + timedelta(
                days=day_offset
            )
            temp_colors[check_date].add(assigned_color)
            if assigned_color not in temp_owners[check_date]:
                temp_owners[check_date][assigned_color] = manip_id

    return True, tentative, None


# --- Experiment splitting ---


def split_pseudorandom_experiment(
    original_experiment_record: dict,
    min_cages_per_manip_per_split: int = 4,
) -> list[dict]:
    """Split a pseudorandom experiment into multiple parts.

    Returns a list of new experiment records representing the splits.
    """
    exp_fields = original_experiment_record.get("fields", {})
    original_cpm = int(exp_fields.get("cages_per_manip", 8))

    if original_cpm < min_cages_per_manip_per_split:
        return []

    max_splits = original_cpm // min_cages_per_manip_per_split
    if max_splits <= 1:
        return []

    num_splits = max_splits
    split_experiments = []
    base_cpm = original_cpm // num_splits
    remainder = original_cpm % num_splits

    for split_index in range(num_splits):
        cages_for_split = base_cpm + (
            1 if split_index < remainder else 0
        )

        split_exp = copy.deepcopy(original_experiment_record)
        split_fields = split_exp.get("fields", {})
        split_fields["cages_per_manip"] = str(cages_for_split)

        # Scale experiment_time proportionally
        original_time = split_fields.get("experiment_time")
        if original_time is not None:
            try:
                original_float = float(original_time)
                factor = cages_for_split / original_cpm
                split_fields["experiment_time"] = (
                    original_float * factor
                )
            except (ValueError, TypeError, ZeroDivisionError):
                split_fields.pop("experiment_time", None)

        original_exp_id = split_fields.get(
            "experiment_id", "unknown"
        )
        split_fields["_is_split"] = True
        split_fields["_split_index"] = split_index + 1
        split_fields["_total_splits"] = num_splits
        split_fields["_original_experiment_id"] = original_exp_id

        split_experiments.append(split_exp)

    return split_experiments
