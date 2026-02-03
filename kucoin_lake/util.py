from __future__ import annotations

import re
from typing import Optional


def parse_tf_minutes(tf: Optional[str]) -> Optional[int]:
    if tf is None:
        return None
    m = re.fullmatch(r"(\d+)([mhd])", tf.strip().lower())
    if not m:
        return None
    n = int(m.group(1))
    u = m.group(2)
    if u == "m":
        return n
    if u == "h":
        return n * 60
    if u == "d":
        return n * 1440
    return None


def expected_rows_for_day(tf: Optional[str]) -> Optional[int]:
    mins = parse_tf_minutes(tf)
    if mins is None or mins <= 0:
        return None
    if 1440 % mins != 0:
        return None
    return 1440 // mins
