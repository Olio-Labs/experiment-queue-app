from __future__ import annotations

from typing import List

from .airtable_base import AirtableBase

CAGES_TABLE = "cages"
BOXES_TABLE = "boxes"


class CagesRepository:
    def __init__(self, base: AirtableBase):
        self._base = base

    def list_all_cages(self) -> List[dict]:
        return self._base.table(CAGES_TABLE).all()

    def list_all_boxes(self) -> List[dict]:
        return self._base.table(BOXES_TABLE).all()
