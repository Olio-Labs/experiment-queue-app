from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from pyairtable import Api


@dataclass
class AirtableBase:
    api_key: str
    base_id: str

    @classmethod
    def from_env(cls) -> "AirtableBase":
        api_key = os.getenv("AIRTABLE_API_KEY", "")
        base_id = os.getenv("AIRTABLE_BASE_ID", "")
        return cls(api_key=api_key, base_id=base_id)

    @property
    def api(self) -> Api:
        return Api(self.api_key)

    def table(self, table_name: str) -> Any:
        return self.api.table(self.base_id, table_name)
