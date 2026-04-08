from __future__ import annotations

from typing import List

from .airtable_base import AirtableBase

MANIPULATIONS_TABLE = 'manipulations'
DRUGS_TABLE = 'drugs'


class ManipulationsRepository:
    def __init__(self, base: AirtableBase):
        self._base = base

    def list_all_manipulations(self) -> List[dict]:
        return self._base.table(MANIPULATIONS_TABLE).all()

    def list_all_drugs(self) -> List[dict]:
        return self._base.table(DRUGS_TABLE).all()


