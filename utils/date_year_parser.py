"""Reusable year extraction helpers for mixed-format date text."""

from __future__ import annotations

import re


YEAR_RE = re.compile(r"(19|20)\d{2}")


def extract_year(value: str) -> int | None:
    match = YEAR_RE.search(value)
    if not match:
        return None
    return int(match.group(0))
