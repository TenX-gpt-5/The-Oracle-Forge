"""
Reusable year extraction helpers for mixed-format date text.

# Usage Example:
#   from utils.date_year_parser import extract_year
#   year = extract_year("Founded in 2021.")
#   print(year) # Returns 2021
"""

from __future__ import annotations

import re


YEAR_RE = re.compile(r"(19|20)\d{2}")


def extract_year(value: str) -> int | None:
    match = YEAR_RE.search(value)
    if not match:
        return None
    return int(match.group(0))
