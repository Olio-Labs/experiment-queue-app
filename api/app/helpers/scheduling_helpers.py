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
from typing import Any, Optional

from ..config import settings
from ..services.notes_parser import parse_notes

logger = logging.getLogger(__name__)

# Constants for Pseudorandom Assignment Logic
USE_EXACT_TWO_BOXES_FOR_SPECIAL = True
AIRTABLE_DATE_FORMAT_STR = "%Y-%m-%d"
AIRTABLE_CAGE_MANIPULATION_HISTORY_FIELD_NAME = "manipulation_history"
WASHOUT_MANIPULATION_STRING = "washout"
MAX_MICE_PER_TECHNICIAN_PER_DAY = 60
DAYS_OF_WEEK_ORDERED = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

# Fallback technician availability (used when Google Calendar
# is unavailable)
TEMP_TECHNICIAN_AVAILABILITY: dict[str, list[tuple[str, int]]] = {
    "Monday": [("Henry", 5)],
    "Tuesday": [("James", 4), ("Angie", 4)],
    "Wednesday": [("Henry", 4), ("James", 4)],
    "Thursday": [("Henry", 4), ("Angie", 4)],
    "Friday": [("Henry", 4), ("Angie", 4), ("Tom", 4)],
    "Saturday": [("Henry", 4), ("Tom", 4)],
    "Sunday": [("Henry", 4), ("Tom", 4)],
}

# Box groups for spatial cage assignment
BOXES_1 = [
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
]
BOXES_2 = [
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    49,
    50,
    51,
    52,
    53,
    54,
    55,
    56,
    84,
    85,
    86,
    87,
    88,
]
BOXES_3 = [
    57,
    58,
    59,
    60,
    61,
    62,
    63,
    64,
    65,
    66,
    67,
    68,
    69,
    70,
    71,
    72,
    73,
    74,
    75,
    76,
    77,
    78,
    79,
    80,
    81,
    82,
    83,
]


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

    return estimate_time_direct_mapping_from_notes(notes, task_times, all_cages_data)


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
            mice_count = cage_record.get("fields", {}).get("n_mice", 0)
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
                    box_id = box_record_id_to_box_id_map.get(box_record_id, "unknown")
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
        if not box_links or not isinstance(box_links, list) or len(box_links) == 0:
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
        return None, ("No manipulations provided to assign_cages_pseudorandomly_py.")

    try:
        cages_per_general_manip = (
            int(cages_per_general_manip_str) if cages_per_general_manip_str else 0
        )
        if cages_per_general_manip < 0:
            raise ValueError("'cages_per_manip' must be non-negative.")
    except ValueError as e:
        return None, (f"Invalid 'cages_per_manip' for general manips: {e}")

    logger.info(
        f"Pseudo assign: ExpID {experiment_record.get('id')}, "
        f"manips={manips_to_assign_custom_ids}, "
        f"vehicle={vehicle_manips_for_special_handling_custom_ids}, "
        f"cages_per_general={cages_per_general_manip}"
    )

    current_available_male = [c for c in potential_cages_pool if c["sex"] == "m"]
    current_available_female = [c for c in potential_cages_pool if c["sex"] == "f"]

    master_assignment_map: dict[str, list[str]] = {}
    cages_booked_this_run: set[str] = set()
    male_used_groups: set = set()
    female_used_groups: set = set()

    for manip_custom_id in manips_to_assign_custom_ids:
        selected_cages: list[str] = []
        is_vehicle = manip_custom_id in vehicle_manips_for_special_handling_custom_ids

        if is_vehicle:
            cages_per_vehicle_str = exp_fields.get("cages_per_vehicle")
            try:
                cages_per_vehicle = (
                    int(cages_per_vehicle_str) if cages_per_vehicle_str else 4
                )
                if cages_per_vehicle < 0:
                    raise ValueError("'cages_per_vehicle' must be non-negative.")
            except ValueError as e:
                return None, (f"Invalid 'cages_per_vehicle': {e}")

            sex_assignment = exp_fields.get("sex_assignment", "evenly_split")
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
                temp_female = select_cages_spatially_with_availability(
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
                selected_cages.extend(temp_female)

            if len(selected_cages) == num_needed:
                master_assignment_map[manip_custom_id] = selected_cages
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

            sex_assignment = exp_fields.get("sex_assignment", "evenly_split")
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
                selected_males = select_cages_spatially_with_availability(
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
                selected_females = select_cages_spatially_with_availability(
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
            selected_cages.extend(selected_females)
            for cid in selected_females:
                cages_booked_this_run.add(cid)
            current_available_female = [
                c
                for c in current_available_female
                if c["airtable_record_id"] not in selected_females
            ]

            if len(selected_cages) == num_needed:
                master_assignment_map[manip_custom_id] = selected_cages
            else:
                return None, (
                    f"Could not assign enough cages for "
                    f"general manip '{manip_custom_id}'. "
                    f"Required: {num_needed}, "
                    f"Got: {len(selected_cages)}"
                )

    if len(master_assignment_map) == len(manips_to_assign_custom_ids):
        logger.info(f"Successfully generated assignment map: {master_assignment_map}")
        return master_assignment_map, None
    else:
        missing = [
            m for m in manips_to_assign_custom_ids if m not in master_assignment_map
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
        return False, None, ("No syringe colors available in master list.")

    colors_to_try = list(available_syringe_colors_master_list)
    random.shuffle(colors_to_try)

    for manip_id in sorted(list(manip_custom_ids_needed)):
        color_assigned = False
        assigned_color: Optional[str] = None

        # 1. Try persistent color
        if manip_id in manip_to_persistent_color_map:
            persistent_color = manip_to_persistent_color_map[manip_id]
            can_use = True
            for day_offset in range(num_days_duration):
                check_date = proposed_start_date + timedelta(days=day_offset)
                global_owner = daily_color_owner_map.get(check_date, {}).get(
                    persistent_color
                )
                temp_owner = temp_owners.get(check_date, {}).get(persistent_color)
                used_globally = persistent_color in (
                    daily_used_syringe_colors.get(check_date, set())
                )
                used_temp = persistent_color in temp_colors.get(check_date, set())

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
                    check_date = proposed_start_date + timedelta(days=day_offset)
                    global_owner = daily_color_owner_map.get(check_date, {}).get(
                        candidate
                    )
                    temp_owner = temp_owners.get(check_date, {}).get(candidate)
                    used_globally = candidate in (
                        daily_used_syringe_colors.get(check_date, set())
                    )
                    used_temp = candidate in temp_colors.get(check_date, set())

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
            return (
                False,
                None,
                (
                    f"[Exp {exp_id_for_log}] No available syringe "
                    f"color for manip '{manip_id}' starting "
                    f"{proposed_start_date}."
                ),
            )

        tentative[manip_id] = assigned_color
        for day_offset in range(num_days_duration):
            check_date = proposed_start_date + timedelta(days=day_offset)
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
        cages_for_split = base_cpm + (1 if split_index < remainder else 0)

        split_exp = copy.deepcopy(original_experiment_record)
        split_fields = split_exp.get("fields", {})
        split_fields["cages_per_manip"] = str(cages_for_split)

        # Scale experiment_time proportionally
        original_time = split_fields.get("experiment_time")
        if original_time is not None:
            try:
                original_float = float(original_time)
                factor = cages_for_split / original_cpm
                split_fields["experiment_time"] = original_float * factor
            except (ValueError, TypeError, ZeroDivisionError):
                split_fields.pop("experiment_time", None)

        original_exp_id = split_fields.get("experiment_id", "unknown")
        split_fields["_is_split"] = True
        split_fields["_split_index"] = split_index + 1
        split_fields["_total_splits"] = num_splits
        split_fields["_original_experiment_id"] = original_exp_id

        split_experiments.append(split_exp)

    return split_experiments


# --- Scheduling support functions ---


def create_cage_to_box_mapping(
    all_cages_data: list[dict],
    all_boxes_data: list[dict],
) -> dict[str, int]:
    """Create a mapping from cage record ID to box group number (1-3)."""
    box_id_to_group: dict[str, int] = {}
    for box_num in BOXES_1:
        box_id_to_group[f"b{box_num:07d}"] = 1
    for box_num in BOXES_2:
        box_id_to_group[f"b{box_num:07d}"] = 2
    for box_num in BOXES_3:
        box_id_to_group[f"b{box_num:07d}"] = 3

    box_record_id_to_box_id: dict[str, str] = {}
    for box_record in all_boxes_data:
        if "id" in box_record and "fields" in box_record:
            box_id = box_record["fields"].get("box_id")
            if box_id:
                box_record_id_to_box_id[box_record["id"]] = box_id

    cage_to_box_group: dict[str, int] = {}
    for cage_record in all_cages_data:
        if "id" in cage_record and "fields" in cage_record:
            cage_record_id = cage_record["id"]
            box_links = cage_record["fields"].get("box", [])
            if box_links and len(box_links) > 0:
                box_record_id = box_links[0]
                box_id = box_record_id_to_box_id.get(box_record_id)
                if box_id:
                    box_group = box_id_to_group.get(box_id)
                    if box_group:
                        cage_to_box_group[cage_record_id] = box_group

    logger.info(f"Created cage-to-box mapping for {len(cage_to_box_group)} cages")
    return cage_to_box_group


def get_technicians_and_capacity_per_day(
    availability_schedule: dict,
) -> dict[str, dict[str, Any]]:
    """Calculate technician counts and mice capacity per day."""
    daily_tech_details: dict[str, dict[str, Any]] = {}
    for day, tech_list in availability_schedule.items():
        unique_techs = set(name for name, hours in tech_list if hours > 0)
        num_techs = len(unique_techs)
        total_hours = sum(hours for _, hours in tech_list)
        daily_tech_details[day] = {
            "num_technicians": num_techs,
            "max_mice": num_techs * MAX_MICE_PER_TECHNICIAN_PER_DAY,
            "total_hours_available": total_hours,
        }
    return daily_tech_details


def sort_experiments_for_scheduling(
    experiments: list[dict],
) -> list[dict]:
    """Sort experiments by priority, assignment type, num_days, creation time."""
    assignment_field = settings.assignment_field_name

    def sort_key(exp: dict) -> tuple:
        fields = exp.get("fields", {})
        priority = fields.get("priority", float("inf"))
        assignment_type = fields.get(assignment_field, "").lower()
        num_days = fields.get("num_days", 1)
        created_str = exp.get("createdTime", "9999-12-31T23:59:59.999Z")
        try:
            if "." in created_str:
                created = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                created = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            created = datetime.max
        assignment_order = 0 if assignment_type == "direct_mapping" else 1
        return (priority, assignment_order, -num_days, created)

    return sorted(experiments, key=sort_key)


def check_technician_resources_for_period(
    experiment_time_per_day_minutes: float,
    num_mice_for_experiment: int,
    start_day_name: str,
    num_days_duration: int,
    tech_availability_schedule: dict,
    daily_tech_time_booked: dict[str, float],
    daily_mice_booked: dict[str, int],
    daily_tech_details_map: dict[str, dict[str, Any]],
) -> tuple[bool, str]:
    """Check if technicians have enough time and mice capacity.

    Returns (True, "") and updates daily_tech_time_booked IN PLACE
    if successful. Returns (False, reason) without modifying if not.
    """
    try:
        start_idx = DAYS_OF_WEEK_ORDERED.index(start_day_name)
    except ValueError:
        return False, "Invalid start day name."

    days_to_check = min(num_days_duration, 7)
    total_time_to_commit: dict[str, float] = {}

    for i in range(days_to_check):
        day_idx = (start_idx + i) % 7
        day_name = DAYS_OF_WEEK_ORDERED[day_idx]

        total_time_to_commit[day_name] = (
            total_time_to_commit.get(day_name, 0) + experiment_time_per_day_minutes
        )

        details = daily_tech_details_map.get(day_name)
        if not details:
            return False, (f"No technicians defined for {day_name}.")

        available_min = details["total_hours_available"] * 60
        already_booked = daily_tech_time_booked.get(day_name, 0)
        needed = total_time_to_commit[day_name]
        remaining = available_min - (already_booked + needed)

        if (already_booked + needed) > available_min:
            return False, (
                f"Not enough tech time on {day_name}. "
                f"Need {needed:.1f}, available: {remaining:.1f}."
            )

        max_mice = details["max_mice"]
        mice_booked = daily_mice_booked.get(day_name, 0)
        if (mice_booked + num_mice_for_experiment) > max_mice:
            return False, (
                f"Not enough mice capacity on {day_name}. "
                f"Need {num_mice_for_experiment}, "
                f"available: {max_mice - mice_booked}."
            )

    for day_name, new_time in total_time_to_commit.items():
        if new_time > 0:
            daily_tech_time_booked[day_name] = (
                daily_tech_time_booked.get(day_name, 0) + new_time
            )

    return True, ""


def extract_washout_violations_from_notes(
    notes: str,
) -> tuple[str, list[str]]:
    """Extract washout violations from notes string.

    Returns (cleaned_notes, list_of_violation_cage_ids).
    """
    marker = "__WASHOUT_VIOLATIONS__:"
    if marker not in notes:
        return notes, []

    parts = notes.split(marker)
    cleaned_notes = parts[0].rstrip()
    violations_str = parts[1].strip() if len(parts) > 1 else ""
    violations = [v.strip() for v in violations_str.split(",") if v.strip()]
    return cleaned_notes, violations


# --- Cage assignment orchestration ---


def check_and_assign_cages_for_period(
    experiment_record: dict,
    proposed_start_date: date,
    all_cages_data: list[dict],
    preview_booked_cages: dict[str, set[date]],
    potential_cage_pool: list[dict] | None = None,
    planner_history: list[dict] | None = None,
    manip_record_id_to_custom_id_map: dict[str, str] | None = None,
    all_manipulations_map: dict[str, dict] | None = None,
    all_drugs_map: dict[str, dict] | None = None,
    effective_last_use_tracker: dict[str, date] | None = None,
    all_boxes_data: list[dict] | None = None,
) -> tuple[list[str] | None, str, str | None]:
    """Orchestrate cage assignment for an experiment.

    Handles both direct_mapping and pseudorandom assignment types.
    Returns (assigned_cage_record_ids, updated_notes, error_or_none).
    """
    from ..domain.experiment import ExperimentContext, ExperimentFactory
    from ..services.cage_availability_service import (
        is_cage_available_on_date,
    )
    from ..services.notes_parser import parse_notes, update_notes_with_mapping

    exp_fields = experiment_record.get("fields", {})
    exp_id = experiment_record.get("id", "UnknownExpID")
    original_notes = exp_fields.get("notes", "")
    assignment_type = exp_fields.get(settings.assignment_field_name, "").strip().lower()
    if not assignment_type:
        return None, original_notes, (f"Experiment {exp_id} missing assignment field")

    num_days = int(exp_fields.get("num_days", 1))
    if num_days <= 0:
        num_days = 1

    def _parse_date(s: object, fmt: str = "%Y-%m-%d") -> date | None:
        if isinstance(s, str) and s.strip():
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except Exception:
                return None
        return None

    if assignment_type == "direct_mapping":
        unique_cage_ids = exp_fields.get("unique_cage_ids", [])
        if unique_cage_ids and not isinstance(unique_cage_ids, list):
            unique_cage_ids = [unique_cage_ids]

        assigned: list[str] = []
        if unique_cage_ids:
            assigned = [str(r) for r in unique_cage_ids if str(r).strip()]
        else:
            raw = exp_fields.get("cage", [])
            if raw and not isinstance(raw, list):
                raw = [raw]
            custom_to_record: dict[str, str] = {}
            for c in all_cages_data or []:
                rid = c.get("id")
                cid = (c.get("fields") or {}).get("cage")
                if rid and cid:
                    custom_to_record[str(cid)] = rid
            for cid in raw or []:
                mapped = custom_to_record.get(str(cid))
                if mapped:
                    assigned.append(mapped)

        if not assigned:
            return (
                None,
                original_notes,
                (
                    "Direct mapping error: No cages linked via "
                    "'unique_cage_ids' or 'cage' fields."
                ),
            )

        for record_id in assigned:
            cage_rec = next(
                (c for c in all_cages_data if c.get("id") == record_id),
                None,
            )
            if not cage_rec or "fields" not in cage_rec:
                return None, original_notes, (f"Cage record '{record_id}' not found.")
            cage_fields = cage_rec["fields"]
            booked = preview_booked_cages.get(record_id, set())
            for d_off in range(num_days):
                sched_day = proposed_start_date + timedelta(days=d_off)
                try:
                    ok = is_cage_available_on_date(
                        cage_fields,
                        record_id,
                        sched_day,
                        booked,
                        _parse_date,
                        AIRTABLE_DATE_FORMAT_STR,
                        AIRTABLE_CAGE_MANIPULATION_HISTORY_FIELD_NAME,
                        WASHOUT_MANIPULATION_STRING,
                        False,
                        planner_history or [],
                    )
                except Exception:
                    ok = False
                if not ok:
                    disp = cage_fields.get("cage", record_id)
                    return (
                        None,
                        original_notes,
                        (f"Cage '{disp}' not available on {sched_day}"),
                    )

        return assigned, original_notes, None

    elif assignment_type == "pseudorandom":
        if not all(
            [
                potential_cage_pool,
                planner_history is not None,
                manip_record_id_to_custom_id_map,
                all_manipulations_map,
                all_drugs_map,
            ]
        ):
            return None, original_notes, ("Pseudorandom: Required data not provided.")

        _, manips_from_notes = parse_notes(original_notes)
        manips_from_notes = [
            str(m) for m in (manips_from_notes or []) if str(m).strip()
        ]

        if not manips_from_notes:
            return [], original_notes, None

        cage_to_box_map: dict[str, int] = {}
        if all_cages_data and all_boxes_data:
            cage_to_box_map = create_cage_to_box_mapping(all_cages_data, all_boxes_data)

        ctx = ExperimentContext(
            scheduling_date=proposed_start_date,
            task_times={},
            cages_pool=potential_cage_pool or [],
            all_cages=all_cages_data or [],
            boxes=all_boxes_data or [],
            planner_history=planner_history or [],
            manip_record_id_to_custom_id=(manip_record_id_to_custom_id_map or {}),
            all_manipulations_map=all_manipulations_map or {},
            all_drugs_map=all_drugs_map or {},
            preview_booked_cages=preview_booked_cages or {},
            effective_last_use=effective_last_use_tracker or {},
            cage_to_box_group_map=cage_to_box_map,
        )

        try:
            exp_obj = ExperimentFactory.from_airtable_record(experiment_record)
            exp_obj.assign_cages(ctx)
            amap = exp_obj.direct_mapping_map or {}
        except Exception as e:
            return None, original_notes, (f"Domain assignment error: {e}")

        flat_ids = [rid for ids in amap.values() for rid in ids]

        cpm = exp_fields.get("cages_per_manip")
        if manips_from_notes and not flat_ids:
            if cpm and int(cpm) > 0:
                return None, original_notes, ("Pseudorandom: No available cages.")

        updated_notes = original_notes
        if potential_cage_pool:
            at_to_custom = {
                c["airtable_record_id"]: c["custom_cage_id"]
                for c in potential_cage_pool
                if "airtable_record_id" in c and "custom_cage_id" in c
            }
            notes_map = {
                mk: [at_to_custom.get(r, r) for r in rids] for mk, rids in amap.items()
            }
            updated_notes = update_notes_with_mapping(original_notes, notes_map)

        return flat_ids, updated_notes, None

    return None, original_notes, (f"Unknown assignment type '{assignment_type}'.")


# --- Drug availability checking ---


def check_drug_availability_for_period(
    experiment_record: dict,
    all_manipulations_map: dict[str, dict],
    all_drug_inventory: list[dict],
    all_drugs_map: dict[str, dict],
    all_cages_data: list[dict] | None = None,
) -> tuple[bool, dict[str, float], str | None]:
    """Check drug inventory against experiment requirements.

    Returns (is_available, debits_dict, error_or_none).
    Drug warnings are soft constraints — they don't block scheduling.
    """
    from ..services.notes_parser import parse_notes

    exp_fields = experiment_record.get("fields", {})
    exp_id = exp_fields.get("experiment_id", experiment_record.get("id", "Unknown"))
    manip_ids = exp_fields.get("unique_manipulation_ids", [])
    num_days = int(exp_fields.get("num_days", 1))
    assignment_type = exp_fields.get(settings.assignment_field_name, "").strip().lower()
    notes = exp_fields.get("notes", "")

    if not manip_ids:
        return True, {}, None

    tentative_debits: dict[str, float] = defaultdict(float)

    inv_by_drug_name: dict[str, list[dict]] = defaultdict(list)
    for item in all_drug_inventory:
        drug_link = (item.get("fields") or {}).get("drug", [])
        if drug_link:
            drug_rec_id = drug_link[0] if isinstance(drug_link, list) else drug_link
            drug_detail = all_drugs_map.get(drug_rec_id)
            if drug_detail and "fields" in drug_detail:
                name = drug_detail["fields"].get("drug", "").strip()
                if name:
                    inv_by_drug_name[name].append(item)

    parsed_map, _ = parse_notes(notes)

    for manip_id in manip_ids:
        manip_detail = all_manipulations_map.get(manip_id)
        if not manip_detail or "fields" not in manip_detail:
            continue

        mf = manip_detail["fields"]
        manip_name = mf.get("manipulation", manip_id)
        drug_ids = mf.get("drugs", [])
        doses = mf.get("dose_mg_kg", [])

        if not drug_ids:
            continue

        is_vehicle = False
        for did in drug_ids:
            dd = all_drugs_map.get(did)
            if dd and "fields" in dd:
                dt = dd["fields"].get("drug_type", [])
                if isinstance(dt, list) and "vehicle" in dt:
                    is_vehicle = True
                    break

        if is_vehicle:
            continue

        if len(drug_ids) != len(doses):
            return False, {}, (f"{exp_id}: Drug/dose mismatch for '{manip_name}'.")

        n_animals = 0
        if assignment_type == "direct_mapping":
            cages = (
                parsed_map.get(manip_name, []) if isinstance(parsed_map, dict) else []
            )
            if not isinstance(cages, list):
                cages = [cages]
            if all_cages_data:
                custom_to_rec = {
                    (c.get("fields") or {}).get("cage"): c
                    for c in all_cages_data
                    if (c.get("fields") or {}).get("cage")
                }
                for cid in cages:
                    rec = custom_to_rec.get(cid)
                    mice = (rec.get("fields") or {}).get("n_mice", 2) if rec else 2
                    try:
                        n_animals += int(mice)
                    except (ValueError, TypeError):
                        n_animals += 2
            else:
                n_animals = len(cages) * 2
        elif assignment_type == "pseudorandom":
            cpm = exp_fields.get("cages_per_manip")
            if cpm is None:
                continue
            try:
                n_animals = int(cpm)
            except (ValueError, TypeError):
                continue
        else:
            continue

        if n_animals == 0:
            continue

        for i, drug_id in enumerate(drug_ids):
            dose_raw = doses[i] if i < len(doses) else None
            drug_name = (
                all_drugs_map.get(drug_id, {}).get("fields", {}).get("drug", drug_id)
            )
            if drug_name and drug_name.lower() == "saline":
                continue
            if dose_raw is None:
                continue
            try:
                dose = float(dose_raw)
            except (ValueError, TypeError):
                return False, {}, (f"{exp_id}: Invalid dose for '{drug_name}'.")
            if dose <= 0:
                continue

            needed = dose * n_animals * num_days * (30 / 1000)
            remaining = needed

            for inv_rec in inv_by_drug_name.get(drug_name, []):
                inv_id = inv_rec["id"]
                stock = inv_rec.get("fields", {}).get("amount_available_mg", 0.0)
                debited = tentative_debits.get(inv_id, 0.0)
                avail = stock - debited
                if avail <= 0:
                    continue
                take = min(remaining, avail)
                tentative_debits[inv_id] += take
                remaining -= take
                if remaining <= 0:
                    break

            if remaining > 0:
                avail_amount = needed - remaining
                return (
                    False,
                    {},
                    (
                        f"{exp_id}: Insufficient '{drug_name}' for "
                        f"'{manip_name}'. Need {needed:.1f}mg, "
                        f"have {avail_amount:.1f}mg."
                    ),
                )

    return True, dict(tentative_debits), None
