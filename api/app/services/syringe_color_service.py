from datetime import date
from typing import Dict, Set

from collections import defaultdict

from app.repositories.planner_repo import PlannerRepository


def get_existing_syringe_colors(
    api_key: str,
    base_id: str,
    planner_table_name: str,
) -> Dict[date, Set[str]]:
    if not api_key or not base_id or not planner_table_name:
        return defaultdict(set)
    repo = PlannerRepository(api_key, base_id, planner_table_name)
    return repo.get_existing_syringe_colors_by_date()


