from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple


def is_cage_available_on_date(
    cage_fields: dict,
    cage_id: str,
    scheduling_day: date,
    preview_booked_dates_for_this_cage: Set[date],
    parse_airtable_date_func,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    is_chronic_experiment: bool = False,
    planner_history_data: Optional[List[Dict]] = None,
) -> bool:
    logger = logging.getLogger(__name__)
    # Log manipulations lookup and last use value as seen on the cage
    try:
        custom_cage_id = (
            cage_fields.get("cage") if isinstance(cage_fields, dict) else None
        )
        manip_list_raw = (
            cage_fields.get(cage_manip_history_field_name)
            if isinstance(cage_fields, dict)
            else None
        )
        last_use_debug = (
            cage_fields.get("date_of_last_use")
            if isinstance(cage_fields, dict)
            else None
        )
        if isinstance(manip_list_raw, list):
            msg = (
                f"MANIP_HISTORY: Cage {custom_cage_id or cage_id} "
                f"field='{cage_manip_history_field_name}' "
                f"len={len(manip_list_raw)} "
                f"tail={manip_list_raw[-8:]}"
            )
            logger.debug(msg)
        else:
            msg = (
                f"MANIP_HISTORY: Cage {custom_cage_id or cage_id} "
                f"field='{cage_manip_history_field_name}' "
                f"type={type(manip_list_raw).__name__} "
                f"value={manip_list_raw}"
            )
            logger.debug(msg)
        logger.debug(
            f"MANIP_HISTORY: Cage {custom_cage_id or cage_id} "
            f"date_of_last_use={last_use_debug}"
        )
    except Exception:
        # Never block availability due to logging issues
        pass
    if scheduling_day in preview_booked_dates_for_this_cage:
        msg = (
            f"CAGE_AVAILABILITY: Cage {cage_id} "
            f"unavailable due to preview booking "
            f"on {scheduling_day}"
        )
        logger.debug(msg)
        return False

    # Chronic exception: experiment_series in planner history
    if is_chronic_experiment and planner_history_data:
        custom_cage_id = cage_fields.get("cage")
        if custom_cage_id:
            cage_entries = [
                entry
                for entry in planner_history_data
                if entry.get("cage_id") == custom_cage_id
            ]
            if cage_entries:
                cage_entries.sort(
                    key=lambda x: x.get("start_date", date.min), reverse=True
                )
                most_recent_entry = cage_entries[0]
                experiment_series = most_recent_entry.get("experiment_series", "")
                if experiment_series:
                    msg = (
                        f"CAGE_AVAILABILITY: Cage {cage_id} "
                        f"has experiment_series "
                        f"'{experiment_series}' and is "
                        f"chronic - SKIPPING WASHOUT"
                    )
                    logger.debug(msg)
                    return True

    # Compute true last injection date by discounting
    # trailing non-injection manipulations
    effective_last_injection_date = compute_effective_last_injection_date_for_cage(
        latest_cage_fields=cage_fields,
        parse_airtable_date_func=parse_airtable_date_func,
        airtable_date_format_str=airtable_date_format_str,
        manip_history_field_name=cage_manip_history_field_name,
        washout_ids={"m0000000", "m0000004"},
    )

    if effective_last_injection_date:
        days_since_effective = (scheduling_day - effective_last_injection_date).days
        msg = (
            f"CAGE_AVAILABILITY: Cage {cage_id} "
            f"effective_last_injection_date="
            f"{effective_last_injection_date}, "
            f"scheduling_day={scheduling_day}, "
            f"days_since_effective="
            f"{days_since_effective}"
        )
        logger.debug(msg)
        # Disallow same-day reuse relative to the last true injection
        if days_since_effective < 1:
            msg = (
                f"CAGE_AVAILABILITY: Cage {cage_id} "
                f"unavailable — same-day as last "
                f"true injection "
                f"({effective_last_injection_date})"
            )
            logger.debug(msg)
            return False

    return True


def is_cage_available_for_direct_mapping(
    cage_fields: dict,
    cage_id: str,
    scheduling_day: date,
    preview_booked_dates_for_this_cage: Set[date],
    parse_airtable_date_func,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    is_chronic_experiment: bool = False,
    planner_history_data: Optional[List[Dict]] = None,
) -> bool:
    logger = logging.getLogger(__name__)
    if scheduling_day in preview_booked_dates_for_this_cage:
        msg = (
            f"DIRECT_MAPPING_AVAILABILITY: "
            f"Cage {cage_id} already booked in "
            f"preview on {scheduling_day}"
        )
        logger.debug(msg)
        return False
    msg = (
        f"DIRECT_MAPPING_AVAILABILITY: "
        f"Cage {cage_id} available on "
        f"{scheduling_day} (washout bypassed)"
    )
    logger.debug(msg)
    return True


def check_if_cage_in_washout(
    cage_fields: dict,
    cage_id: str,
    scheduling_day: date,
    parse_airtable_date_func,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
) -> bool:
    last_use_val = cage_fields.get("date_of_last_use")
    actual_date_str = None
    if isinstance(last_use_val, list):
        if last_use_val and isinstance(last_use_val[0], str):
            actual_date_str = last_use_val[0]
    elif isinstance(last_use_val, str):
        actual_date_str = last_use_val

    last_use_date = parse_airtable_date_func(actual_date_str, airtable_date_format_str)
    if last_use_date and not isinstance(last_use_date, date):
        last_use_date = last_use_date.date()

    if last_use_date:
        days_since_last_use = (scheduling_day - last_use_date).days
        if days_since_last_use < 1:
            past_manipulations = cage_fields.get(cage_manip_history_field_name, [])
            if not isinstance(past_manipulations, list):
                return False
            num_past_manips = len(past_manipulations)
            if num_past_manips >= 2:
                if not (
                    (
                        past_manipulations[-1] == washout_manip_str
                        or past_manipulations[-1] == "m0000004"
                    )
                    and (
                        past_manipulations[-2] == washout_manip_str
                        or past_manipulations[-2] == "m0000004"
                    )
                ):
                    return True
            elif num_past_manips == 1:
                if not (
                    past_manipulations[-1] == washout_manip_str
                    or past_manipulations[-1] == "m0000004"
                ):
                    return True
            else:
                return True
    return False


def compute_effective_last_injection_date_for_cage(
    latest_cage_fields: Dict,
    parse_airtable_date_func: callable,
    airtable_date_format_str: str,
    manip_history_field_name: str,
    washout_ids: Optional[Set[str]] = None,
) -> Optional[date]:
    logger = logging.getLogger(__name__)
    if washout_ids is None:
        washout_ids = {"m0000000", "m0000004"}

    last_use_val = latest_cage_fields.get("date_of_last_use")
    date_str_to_parse = None
    if isinstance(last_use_val, list):
        if last_use_val and isinstance(last_use_val[0], str):
            date_str_to_parse = last_use_val[0]
    elif isinstance(last_use_val, str):
        date_str_to_parse = last_use_val

    if not date_str_to_parse:
        logger.warning(
            "EFFECTIVE_LAST_USE: Missing "
            "'date_of_last_use' on cage; "
            "cannot derive effective date"
        )
        return None

    parsed_date = parse_airtable_date_func(date_str_to_parse, airtable_date_format_str)
    if parsed_date and not isinstance(parsed_date, date):
        parsed_date = parsed_date.date()
    if not parsed_date:
        msg = (
            f"EFFECTIVE_LAST_USE: Could not parse "
            f"date_of_last_use='{date_str_to_parse}'"
        )
        logger.warning(msg)
        return None

    manip_list = latest_cage_fields.get(manip_history_field_name, [])
    if not isinstance(manip_list, list):
        logger.warning(
            "EFFECTIVE_LAST_USE: Manip history "
            "field is not a list; "
            "cannot derive effective date"
        )
        return None

    # Walk backwards over manipulations; subtract 1 day
    # for each trailing non-injection manip
    trailing_non_injection_days = 0
    for manip in reversed(manip_list):
        if manip in washout_ids:
            trailing_non_injection_days += 1
        else:
            break

    effective_date = parsed_date - timedelta(days=trailing_non_injection_days)
    msg = (
        f"EFFECTIVE_LAST_USE: "
        f"cage={latest_cage_fields.get('cage')} "
        f"last_use={parsed_date} "
        f"trail_non_inj_days="
        f"{trailing_non_injection_days} "
        f"effective={effective_date}"
    )
    logger.debug(msg)
    return effective_date


def calculate_cage_availability_score(
    cage_fields: Dict,
    check_date: date,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
) -> float:
    """
    Calculate a utilization score for a cage on a specific date.
    0.0: Available
    0.5: In washout
    1.0: Used today / unavailable
    """
    last_use_date_value = cage_fields.get("date_of_last_use")

    # Parse the last use date
    if isinstance(last_use_date_value, list) and last_use_date_value:
        last_use_date_str = (
            last_use_date_value[0] if isinstance(last_use_date_value[0], str) else None
        )
    elif isinstance(last_use_date_value, str):
        last_use_date_str = last_use_date_value
    else:
        last_use_date_str = None

    if not last_use_date_str:
        return 0.0

    try:
        last_use_date = datetime.strptime(last_use_date_str, "%Y-%m-%d").date()
        hours_since_last_use = (check_date - last_use_date).days * 24

        if hours_since_last_use < 0:
            # Future last use; check original history
            # not available here → assume available
            return 0.0
        elif hours_since_last_use == 0:
            return 1.0
        elif hours_since_last_use < 72:
            manip_history = cage_fields.get(cage_manip_history_field_name, [])
            if isinstance(manip_history, list) and len(manip_history) >= 2:
                last_two = manip_history[-2:]
                if all(m == washout_manip_str or m == "m0000004" for m in last_two):
                    return 0.0
            return 0.5
        else:
            return 0.0
    except (ValueError, TypeError):
        return 0.0


def select_cages_spatially_with_availability(
    manip_id_to_assign: str,
    num_cages_to_select: int,
    candidate_cages: List[Dict[str, str]],
    planner_history: List[Dict],
    current_scheduling_date: date,
    num_days_duration: int,
    preview_booked_cages: Dict[str, Set[date]],
    cages_booked_within_current_experiment_run: Set[str],
    is_cage_available_func: callable,
    parse_airtable_date_func: callable,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    live_all_cages_details_for_availability_check: List[Dict],
    effective_last_use_tracker: Dict[str, date],
    cage_to_box_group_map: Dict[str, int],
    used_box_groups: set,
) -> List[str]:
    """
    Spatially-aware selection:
    - Filter candidates by availability for all days
    - Sort by oldest effective last use (fairness)
    - Greedy round-robin across box groups using
      cage_to_box_group_map; falls back if a group
      is exhausted
    - Respects cages_booked_within_current_experiment_run
    """
    if num_cages_to_select <= 0 or not candidate_cages:
        return []

    cages = live_all_cages_details_for_availability_check
    live_index = {c.get("id"): c for c in cages}

    def _is_available_all_days(fields: dict, cage_id: str) -> bool:
        booked_dates = preview_booked_cages.get(cage_id, set())
        for day_offset in range(num_days_duration):
            check_date = current_scheduling_date + timedelta(days=day_offset)
            try:
                ok = is_cage_available_func(
                    fields,
                    cage_id,
                    check_date,
                    booked_dates,
                    parse_airtable_date_func,
                    airtable_date_format_str,
                    cage_manip_history_field_name,
                    washout_manip_str,
                    False,
                    planner_history,
                )
            except TypeError:
                ok = is_cage_available_func(
                    fields,
                    cage_id,
                    check_date,
                    booked_dates,
                    parse_airtable_date_func,
                    airtable_date_format_str,
                    cage_manip_history_field_name,
                    washout_manip_str,
                )
            if not ok:
                return False
        return True

    # Build list of (cage_id, eff_last_date, box_group)
    eligible: List[Tuple[str, date, Optional[int]]] = []
    for c in candidate_cages:
        cid = c.get("airtable_record_id")
        if not cid or cid in cages_booked_within_current_experiment_run:
            continue
        latest = live_index.get(cid) or {}
        fields = latest.get("fields", {})
        if not _is_available_all_days(fields, cid):
            continue
        eff = effective_last_use_tracker.get(cid)
        if eff is None:
            try:
                eff = (
                    compute_effective_last_injection_date_for_cage(
                        fields,
                        parse_airtable_date_func,
                        airtable_date_format_str,
                        cage_manip_history_field_name,
                    )
                    or date.min
                )
            except Exception:
                eff = date.min
            effective_last_use_tracker[cid] = eff
        grp = cage_to_box_group_map.get(cid)
        eligible.append((cid, eff, grp))

    # Sort by oldest effective last use (ascending date)
    eligible.sort(key=lambda x: x[1] or date.min)

    # Partition by group preserving order
    by_group: Dict[Optional[int], List[str]] = {}
    for cid, _eff, grp in eligible:
        by_group.setdefault(grp, []).append(cid)

    # Start round-robin from the least-used group so far
    group_usage: Dict[Optional[int], int] = {g: 0 for g in by_group.keys()}
    # Include previously used groups to bias away if provided
    for g in used_box_groups:
        group_usage[g] = group_usage.get(g, 0) + 1

    selected: List[str] = []
    groups_cycle = list(by_group.keys())
    # Sort groups by current usage (lowest first) then group id for stability
    groups_cycle.sort(
        key=lambda g: (group_usage.get(g, 0), (g if isinstance(g, int) else 999))
    )

    idx = 0
    while len(selected) < num_cages_to_select and any(by_group.values()):
        if idx >= len(groups_cycle):
            # Re-sort as usage has changed
            groups_cycle.sort(
                key=lambda g: (
                    group_usage.get(g, 0),
                    (g if isinstance(g, int) else 999),
                )
            )
            idx = 0
        g = groups_cycle[idx]
        bucket = by_group.get(g, [])
        if bucket:
            cid = bucket.pop(0)
            selected.append(cid)
            group_usage[g] = group_usage.get(g, 0) + 1
        idx += 1

    # If still short, fill greedily from remaining regardless of group
    if len(selected) < num_cages_to_select:
        remaining = [cid for lst in by_group.values() for cid in lst]
        need = num_cages_to_select - len(selected)
        selected.extend(remaining[:need])

    # Spatial diagnostics per manipulation
    try:
        logger = logging.getLogger(__name__)
        group_counts: Dict[Optional[int], int] = {}
        for cid in selected:
            g = cage_to_box_group_map.get(cid)
            group_counts[g] = group_counts.get(g, 0) + 1
        unique_groups = len([g for g, c in group_counts.items() if c > 0])
        ratio = (unique_groups / len(selected)) if selected else 0.0
        msg = (
            f"SPATIAL_SUMMARY: "
            f"manip={manip_id_to_assign} "
            f"selected={len(selected)} "
            f"unique_groups={unique_groups} "
            f"ratio={ratio:.3f} "
            f"group_counts={group_counts}"
        )
        logger.info(msg)
    except Exception:
        pass

    return selected


def select_cages_by_recency_and_availability(
    manip_id_to_assign: str,
    num_cages_to_select: int,
    candidate_cages: List[Dict[str, str]],
    planner_history: List[Dict],
    current_scheduling_date: date,
    num_days_duration: int,
    preview_booked_cages: Dict[str, Set[date]],
    cages_booked_within_current_experiment_run: Set[str],
    is_cage_available_func: callable,
    parse_airtable_date_func: callable,
    airtable_date_format_str: str,
    cage_manip_history_field_name: str,
    washout_manip_str: str,
    live_all_cages_details_for_availability_check: List[Dict],
    effective_last_use_tracker: Dict[str, date],
) -> List[str]:
    # Faithful behavior: check availability for all
    # days in duration; adapt to 8-arg or 10-arg
    # availability call
    available: List[Tuple[str, date]] = []
    cages = live_all_cages_details_for_availability_check
    live_index = {c.get("id"): c for c in cages}

    def _is_available_all_days(fields: dict, cage_id: str) -> bool:
        booked_dates = preview_booked_cages.get(cage_id, set())
        for day_offset in range(num_days_duration):
            check_date = current_scheduling_date + timedelta(days=day_offset)
            # Try 10-arg call; fallback to 8-arg
            # signature if provided function expects it
            try:
                ok = is_cage_available_func(
                    fields,
                    cage_id,
                    check_date,
                    booked_dates,
                    parse_airtable_date_func,
                    airtable_date_format_str,
                    cage_manip_history_field_name,
                    washout_manip_str,
                    False,
                    planner_history,
                )
            except TypeError:
                ok = is_cage_available_func(
                    fields,
                    cage_id,
                    check_date,
                    booked_dates,
                    parse_airtable_date_func,
                    airtable_date_format_str,
                    cage_manip_history_field_name,
                    washout_manip_str,
                )
            if not ok:
                return False
        return True

    for cage_dict in candidate_cages:
        cage_airtable_id = cage_dict["airtable_record_id"]
        if cage_airtable_id in cages_booked_within_current_experiment_run:
            continue
        latest = live_index.get(cage_airtable_id)
        if not latest or "fields" not in latest:
            continue
        fields = latest["fields"]

        if not _is_available_all_days(fields, cage_airtable_id):
            continue

        eff = effective_last_use_tracker.get(cage_airtable_id)
        if eff is None:
            eff = (
                compute_effective_last_injection_date_for_cage(
                    fields,
                    parse_airtable_date_func,
                    airtable_date_format_str,
                    cage_manip_history_field_name,
                )
                or date.min
            )
            effective_last_use_tracker[cage_airtable_id] = eff
        available.append((cage_airtable_id, eff))

    available.sort(key=lambda x: x[1] or date.min)
    return [cid for cid, _ in available[: max(0, num_cages_to_select)]]


# Backwards-compatible aliases for existing function names in app.py
def is_cage_available_on_specific_date(*args, **kwargs) -> bool:  # type: ignore
    return is_cage_available_on_date(*args, **kwargs)
