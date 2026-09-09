from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence


@dataclass(frozen=True)
class ScanScope:
    symbols: Optional[Sequence[str]] = None
    date_start: Optional[date] = None
    date_end: Optional[date] = None

    @property
    def is_scoped(self) -> bool:
        return bool(self.symbols or self.date_start or self.date_end)

