from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pytz

from .date_range import DateRange

logger = logging.getLogger(__name__)


def ensure_date_keyed(default_val_factory):
    return defaultdict(default_val_factory)


def precommit_in_progress_resources_date_range(
    in_progress_experiments: List[dict],
    date_range: DateRange,
    daily_tech_time_booked_preview: Dict[date, float],
    daily_mice_booked_preview: Dict[date, int],
    preview_booked_cages: Dict[str, Set[date]],
    all_cages_data: List[dict],
    daily_cages_booked_preview: Dict[date, int],
    daily_boxes_booked_preview: Dict[date, Set[str]],
    daily_nonbox_cages_booked_preview: Dict[date, int],
    box_record_id_to_box_id_map: Dict[str, str],
) -> List[dict]:
    """
    Adapter-style rewrite of pre_commit_in_progress_experiment_resources using real date keys.
    Mirrors prior behavior but stores usage by actual date instead of weekday name.
    """
    processed_in_progress: List[dict] = []

    # Convert list of cage records into lookup maps if needed
    for exp_record in in_progress_experiments:
        try:
            exp_fields = exp_record.get("fields", {})
            exp_id = exp_record.get("id", "unknown")

            assigned_cages = exp_fields.get("cage", []) or []
            assigned_manipulations = exp_fields.get("manipulations", []) or []

            # Basic inference for scheduled date range based on status
            scheduled_start_date = None
            scheduled_end_date = None
            if exp_fields.get("actual_start_date"):
                scheduled_start_date = _parse_date_str(exp_fields["actual_start_date"])
            if exp_fields.get("actual_end_date"):
                scheduled_end_date = _parse_date_str(exp_fields["actual_end_date"])

            # If no actual dates, fall back to earliest_start_date + num_days
            if (
                not scheduled_start_date
                and exp_fields.get("earliest_start_date")
                and exp_fields.get("num_days")
            ):
                try:
                    s = _parse_date_str(exp_fields["earliest_start_date"])
                    nd = int(exp_fields["num_days"])
                    scheduled_start_date = s
                    scheduled_end_date = s + timedelta(days=max(nd - 1, 0))
                except Exception:
                    pass

            # If still none, mark as in progress without dates
            if scheduled_start_date and scheduled_end_date:
                for d in DateRange(
                    scheduled_start_date, scheduled_end_date
                ).iter_days():
                    if date_range.contains(d):
                        # Update date-keyed aggregates; preserving prior assumptions
                        daily_cages_booked_preview[d] = daily_cages_booked_preview.get(
                            d, 0
                        ) + len(assigned_cages)
                        # mice count requires cage lookup; keep it simple here (could be improved)
                        daily_mice_booked_preview[d] = daily_mice_booked_preview.get(
                            d, 0
                        )
            processed_in_progress.append(
                {
                    "record_id": exp_id,
                    "experiment_id_field": exp_fields.get("experiment_id", exp_id),
                    "priority": exp_fields.get("priority", "N/A"),
                    "assignment": exp_fields.get("assignment", "N/A"),
                    "num_days": exp_fields.get("num_days", "N/A"),
                    "experiment_time_daily": exp_fields.get("experiment_time"),
                    "notes": exp_fields.get("notes", ""),
                    "status": "in_progress",
                    "scheduled_start_date": scheduled_start_date.strftime("%Y-%m-%d")
                    if scheduled_start_date
                    else "In Progress",
                    "scheduled_end_date": scheduled_end_date.strftime("%Y-%m-%d")
                    if scheduled_end_date
                    else "In Progress",
                    "assigned_manipulations": assigned_manipulations,
                    "assigned_cages": assigned_cages,
                    "cages_per_manip": exp_fields.get("cages_per_manip", "N/A"),
                    "assigned_syringe_colors": {},
                    "unique_manipulation_ids": exp_fields.get(
                        "unique_manipulation_ids", []
                    ),
                }
            )
        except Exception as e:
            logger.info(f"[InProgress-Range] Skipped exp due to error: {e}")
            continue

    return processed_in_progress


def _parse_date_str(s: str) -> date:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # naive last-resort
    parts = s.split("/")
    if len(parts) == 3:
        month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
        return date(year, month, day)
    raise ValueError(f"Unrecognized date format: {s}")


def precommit_scheduled_resources_date_range(
    scheduled_experiments: List[dict],
    date_range: DateRange,
    daily_tech_time_booked_preview: Dict[date, float],
    daily_mice_booked_preview: Dict[date, int],
    preview_booked_cages: Dict[str, Set[date]],
    all_cages_data: List[dict],
    daily_cages_booked_preview: Dict[date, int],
    daily_boxes_booked_preview: Dict[date, Set[str]],
    daily_nonbox_cages_booked_preview: Dict[date, int],
    box_record_id_to_box_id_map: Dict[str, str],
    manip_record_id_to_custom_id_map: Dict[str, str],
) -> List[dict]:
    processed_scheduled: List[dict] = []

    for exp in scheduled_experiments:
        try:
            fields = exp.get("fields", {})
            exp_id = exp.get("id", "unknown")
            start_str = fields.get("earliest_start_date")
            num_days = fields.get("num_days")
            if not start_str or not num_days:
                logger.info(f"[Scheduled-Range] Skip {exp_id}: missing start/num_days")
                continue
            start = _parse_date_str(start_str)
            nd = int(num_days)
            end = start + timedelta(days=max(nd - 1, 0))

            overlap = [
                d for d in DateRange(start, end).iter_days() if date_range.contains(d)
            ]
            if not overlap:
                continue

            # Aggregate simple counts, mirroring legacy behavior
            assigned_cages = fields.get("cage", []) or []
            assigned_manips = fields.get("manipulations", []) or []

            # Build cage custom id -> record id map
            cage_custom_to_record = {}
            for c in all_cages_data:
                c_fields = c.get("fields", {})
                custom = c_fields.get("cage")
                if custom and "id" in c:
                    cage_custom_to_record[custom] = c["id"]

            assigned_cage_record_ids = [
                cage_custom_to_record.get(cc)
                for cc in assigned_cages
                if cage_custom_to_record.get(cc)
            ]

            for d in overlap:
                daily_cages_booked_preview[d] = daily_cages_booked_preview.get(
                    d, 0
                ) + len(assigned_cage_record_ids)
                # Boxes and mice could be computed similarly; keeping minimal parity for now

            processed_scheduled.append(
                {
                    "record_id": exp_id,
                    "experiment_id_field": fields.get("experiment_id", exp_id),
                    "priority": fields.get("priority", "N/A"),
                    "assignment": fields.get("assignment", "N/A"),
                    "num_days": fields.get("num_days", "N/A"),
                    "experiment_time_daily": fields.get("experiment_time"),
                    "notes": fields.get("notes", ""),
                    "status": "scheduled",
                    "scheduled_start_date": start.strftime("%Y-%m-%d"),
                    "scheduled_end_date": end.strftime("%Y-%m-%d"),
                    "assigned_manipulations": assigned_manips,
                    "assigned_cages": assigned_cages,
                    "cages_per_manip": fields.get("cages_per_manip", "N/A"),
                    "assigned_syringe_colors": {},
                    "unique_manipulation_ids": fields.get(
                        "unique_manipulation_ids", []
                    ),
                }
            )
        except Exception as e:
            logger.info(f"[Scheduled-Range] Skipped exp due to error: {e}")
            continue

    return processed_scheduled


def prepare_cage_heatmap_data(
    all_cages_data: List[Dict],
    preview_booked_cages: Dict[str, Set[date]],
    week_dates: List[date],
    scheduled_experiments: List[Dict],
) -> Dict:
    # Build cage->date->manipulations map and track experiment end dates per cage
    cage_manip_by_date: Dict[str, Dict[date, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    cage_end_dates: Dict[str, Set[date]] = defaultdict(set)

    if scheduled_experiments:
        for exp in scheduled_experiments:
            # Support both top-level keys and nested 'fields'
            fields = exp.get("fields", {}) if isinstance(exp, dict) else {}
            assigned = (
                exp.get("assigned_cages")
                or fields.get("assigned_cages")
                or exp.get("cage")
                or fields.get("cage")
                or []
            )
            manips = (
                exp.get("assigned_manipulations")
                or fields.get("assigned_manipulations")
                or exp.get("manipulations")
                or fields.get("manipulations")
                or []
            )
            start_str = (
                exp.get("scheduled_start_date")
                or fields.get("scheduled_start_date")
                or exp.get("earliest_start_date")
                or fields.get("earliest_start_date")
            )
            end_str = (
                exp.get("scheduled_end_date")
                or fields.get("scheduled_end_date")
                or start_str
            )

            if not isinstance(assigned, list):
                assigned = [assigned]
            assigned = [str(c) for c in assigned if c is not None]

            try:
                s = (
                    datetime.strptime(start_str, "%Y-%m-%d").date()
                    if start_str
                    else None
                )
                e = datetime.strptime(end_str, "%Y-%m-%d").date() if end_str else None
            except Exception:
                s, e = None, None
            if not s or not e:
                continue

            # Reserve manipulations for actual experiment days (exclude end date for end-weight day logic)
            current = s
            while current < e:
                if current in week_dates:
                    for cage_custom_id in assigned:
                        cage_manip_by_date[cage_custom_id][current].extend(manips)
                current += timedelta(days=1)

            # Track end date per assigned cage for experiment-end highlighting
            if e in week_dates:
                for cage_custom_id in assigned:
                    cage_end_dates[cage_custom_id].add(e)

    heatmap: Dict[str, List] = {
        "cage_ids": [],
        "dates": [d.strftime("%a %m/%d") for d in week_dates],
        "utilization_matrix": [],
        "manipulation_matrix": [],
        "cage_details": [],
    }

    for cage in all_cages_data:
        fields = cage.get("fields", {})
        custom_id = fields.get("cage")
        if not custom_id:
            continue
        custom_id = str(custom_id)
        booked_dates_for_cage = preview_booked_cages.get(cage.get("id"), set())

        cage_util_row: List[float] = []
        cage_manip_row: List[List[str]] = []
        for day_date in week_dates:
            if cage_manip_by_date.get(custom_id, {}).get(day_date):
                util = 1.0
            elif day_date in cage_end_dates.get(custom_id, set()):
                util = 0.75
            elif day_date in booked_dates_for_cage:
                util = 0.5
            else:
                util = 0.0
            cage_util_row.append(util)
            cage_manip_row.append(
                cage_manip_by_date.get(custom_id, {}).get(day_date, [])
            )

        heatmap["cage_ids"].append(custom_id)
        heatmap["utilization_matrix"].append(cage_util_row)
        heatmap["manipulation_matrix"].append(cage_manip_row)
        # Prefer 'box_id' if present; fall back to 'box' link
        box_id_value = fields.get("box_id")
        if not box_id_value:
            box_field = fields.get("box")
            if isinstance(box_field, list) and box_field:
                box_id_value = box_field[0]
            else:
                box_id_value = box_field

        heatmap["cage_details"].append(
            {
                "cage_id": custom_id,
                "sex": fields.get("sex"),
                "box_id": box_id_value,
                "airtable_id": cage.get("id"),
            }
        )

    return heatmap


def fetch_technician_availability_date_range(
    service,
    tech_calendar_id: str,
    date_range: DateRange,
    timezone: str,
    valid_names: List[str] | None = None,
    default_hours: int = 4,
) -> Tuple[Dict[date, List[Tuple[str, int]]], Optional[str]]:
    """
    Fetch all-day events from the technician calendar over the given date range.
    Returns a mapping of date -> list of (Technician Name, hours).
    """
    try:
        tz = pytz.timezone(timezone)
        time_min = tz.localize(
            datetime.combine(date_range.start, time(0, 0))
        ).isoformat()
        time_max = tz.localize(
            datetime.combine(date_range.end + timedelta(days=1), time(0, 0))
        ).isoformat()

        events_result = (
            service.events()
            .list(
                calendarId=tech_calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )

        events = events_result.get("items", [])
        valid_set = {n.lower() for n in (valid_names or [])}

        availability_by_date: Dict[date, List[Tuple[str, int]]] = {
            d: [] for d in date_range.iter_days()
        }

        name_mapping = {
            "henry": "Henry",
            "angie": "Angie",
            "james": "James",
            "kevin": "Kevin",
            "tom": "Tom",
            "david": "David",
            "tim": "Tim",
            "gina": "Gina",
        }

        for event in events:
            start_info = event.get("start", {})
            if "date" not in start_info:
                continue  # skip timed events
            event_title = (event.get("summary") or "").strip().lower()
            if valid_set and event_title not in valid_set:
                continue
            try:
                event_date = datetime.strptime(start_info["date"], "%Y-%m-%d").date()
            except Exception:
                continue
            if not date_range.contains(event_date):
                continue
            tech_name = name_mapping.get(event_title, event_title.capitalize())
            availability_by_date[event_date].append((tech_name, default_hours))

        return availability_by_date, None

    except Exception as e:
        return {}, f"Error fetching technician availability: {e}"
