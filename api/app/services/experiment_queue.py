from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from ..domain.experiment import Experiment, ExperimentFactory
from ..repositories.airtable_base import AirtableBase
from ..repositories.experiments_repo import ExperimentsRepository
from ..services.notes_parser import parse_notes


@dataclass
class ExperimentQueue:
    experiments: List[Experiment]

    def sorted_by_priority_then_start(self) -> List[Experiment]:
        def sort_key(exp: Experiment):
            # Prioritize lower priority numbers (1 is highest),
            # then by earliest_start_date
            start = exp.earliest_start_date
            # Ensure priority is an int; treat missing as very low priority
            try:
                pr = int(exp.priority)
            except Exception:
                pr = 9999
            return (pr, start is None, start or date.max)

        return sorted(self.experiments, key=sort_key)

    def sorted_by_actual_then_priority(self) -> List[Experiment]:
        def sort_key(exp: Experiment):
            actual = exp.actual_start_date
            try:
                pr = int(exp.priority)
            except Exception:
                pr = 9999
            return (actual is None, actual or date.max, pr)

        return sorted(self.experiments, key=sort_key)


def load_experiment_queue_from_airtable(
    base: AirtableBase | None = None,
) -> ExperimentQueue:
    base = base or AirtableBase.from_env()
    repo = ExperimentsRepository(base)
    exps = repo.list_all()
    return ExperimentQueue(experiments=exps)


def load_experiment_queue_from_records(records: list[dict]) -> ExperimentQueue:
    experiments: list[Experiment] = []
    for rec in records:
        try:
            exp = ExperimentFactory.from_airtable_record(rec)
            direct_map, manip_ids = parse_notes(exp.notes)
            if direct_map:
                exp.direct_mapping_map = direct_map
                exp.cage_ids = [cid for lst in direct_map.values() for cid in lst]
                exp.manipulation_ids = list(direct_map.keys())
            if manip_ids:
                exp.manipulation_ids = manip_ids
            experiments.append(exp)
        except Exception:
            # Skip malformed records in this loader;
            # upstream already renders raw records
            continue
    return ExperimentQueue(experiments=experiments)
