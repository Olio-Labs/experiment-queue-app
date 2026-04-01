from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Iterator, List


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("DateRange end must be on or after start")

    def contains(self, d: date) -> bool:
        return self.start <= d <= self.end

    def iter_days(self) -> Iterator[date]:
        current = self.start
        while current <= self.end:
            yield current
            current = current + timedelta(days=1)

    @property
    def days(self) -> List[date]:
        return list(self.iter_days())


