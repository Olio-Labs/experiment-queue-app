from datetime import date
from typing import Dict, List, Set


def prepare_tech_workload_chart(
    week_dates: List[date],
    days_of_week_ordered: List[str],
    daily_tech_time_booked_preview: Dict[str, float],  # minutes
    daily_tech_details: Dict[str, Dict[str, float]],
) -> Dict[str, List[float]]:
    labels = [d.strftime('%a %Y-%m-%d') for d in week_dates]
    booked_hours = [
        (daily_tech_time_booked_preview.get(days_of_week_ordered[d.weekday()], 0.0) or 0.0) / 60.0
        for d in week_dates
    ]
    available_hours = [
        (daily_tech_details.get(days_of_week_ordered[d.weekday()], {}).get('total_hours_available', 0.0) or 0.0)
        for d in week_dates
    ]
    return {
        'labels': labels,
        'booked_hours': booked_hours,
        'available_hours': available_hours,
    }


def prepare_cage_usage_chart(
    week_dates: List[date],
    days_of_week_ordered: List[str],
    daily_mice_booked_preview: Dict[str, int],
    daily_cages_booked_preview: Dict[str, int],
    daily_boxes_booked_preview: Dict[str, Set[str]],
    daily_nonbox_cages_booked_preview: Dict[str, int],
    daily_tech_details: Dict[str, Dict[str, float]],
) -> Dict[str, List[float]]:
    labels = [d.strftime('%a %Y-%m-%d') for d in week_dates]
    booked_injections = [
        float(daily_mice_booked_preview.get(days_of_week_ordered[d.weekday()], 0) or 0)
        for d in week_dates
    ]
    capacity_injections = [
        float(daily_tech_details.get(days_of_week_ordered[d.weekday()], {}).get('max_mice', 0) or 0)
        for d in week_dates
    ]
    booked_cages = [
        float(daily_cages_booked_preview.get(days_of_week_ordered[d.weekday()], 0) or 0)
        for d in week_dates
    ]
    booked_boxes = [
        float(len(daily_boxes_booked_preview.get(days_of_week_ordered[d.weekday()], set()) or set()))
        for d in week_dates
    ]
    booked_nonbox_cages = [
        float(daily_nonbox_cages_booked_preview.get(days_of_week_ordered[d.weekday()], 0) or 0)
        for d in week_dates
    ]

    return {
        'labels': labels,
        'booked_injections': booked_injections,
        'capacity_injections': capacity_injections,
        'booked_cages': booked_cages,
        'booked_boxes': booked_boxes,
        'booked_nonbox_cages': booked_nonbox_cages,
    }


