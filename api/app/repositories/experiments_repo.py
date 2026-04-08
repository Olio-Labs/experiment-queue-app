from __future__ import annotations

from typing import List

from ..domain.experiment import Experiment, ExperimentFactory
from ..services.notes_parser import parse_notes
from .airtable_base import AirtableBase

EXPERIMENT_QUEUE_TABLE = 'experiment_queue'


class ExperimentsRepository:
    def __init__(self, base: AirtableBase):
        self._base = base

    def list_all(self) -> List[Experiment]:
        tbl = self._base.table(EXPERIMENT_QUEUE_TABLE)
        records = tbl.all()
        return [self._to_domain(r) for r in records]

    def list_active(self) -> List[Experiment]:
        """
        Return experiments excluding those with status in {'done','hold'}.
        Tries Airtable formula first, falls back to client-side filtering.
        """
        tbl = self._base.table(EXPERIMENT_QUEUE_TABLE)
        try:
            # Exclude both done and hold
            records = tbl.all(formula="AND(NOT({status}='done'),NOT({status}='hold'))")
        except Exception:
            records = tbl.all()
            # Fallback filtering
            records = [
                r for r in records
                if (r.get('fields', {}).get('status', '').strip().lower() not in {'done', 'hold'})
            ]
        return [self._to_domain(r) for r in records]

    def list_in_progress(self) -> List[Experiment]:
        tbl = self._base.table(EXPERIMENT_QUEUE_TABLE)
        # Align with existing filterByFormula semantics later if needed
        records = tbl.all()
        return [self._to_domain(r) for r in records if self._is_in_progress(r)]

    def _to_domain(self, record: dict) -> Experiment:
        exp = ExperimentFactory.from_airtable_record(record)
        # Parse notes for mapping/manips and populate
        direct_map, manip_ids = parse_notes(exp.notes)
        if direct_map:
            exp.direct_mapping_map = direct_map
            # flatten cage ids list for convenience
            exp.cage_ids = [cid for lst in direct_map.values() for cid in lst]
            exp.manipulation_ids = list(direct_map.keys())
        if manip_ids:
            exp.manipulation_ids = manip_ids
        return exp

    def _is_in_progress(self, record: dict) -> bool:
        fields = record.get('fields', {})
        status = (fields.get('status') or '').strip().lower()
        return status in {'in-progress', 'in progress', 'running'}


