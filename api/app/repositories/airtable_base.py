from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyairtable import Api

from ..config import settings


@dataclass
class AirtableBase:
    api_key: str
    base_id: str

    @classmethod
    def from_env(cls) -> "AirtableBase":
        return cls(api_key=settings.airtable_api_key, base_id=settings.airtable_base_id)

    @property
    def api(self) -> Api:
        return Api(self.api_key)

    def table(self, table_name: str) -> Any:
        return self.api.table(self.base_id, table_name)
