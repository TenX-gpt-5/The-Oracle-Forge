"""
join_key_store.py

Normalization rules and ID mapping hints for cross-database joins.
"""

from __future__ import annotations

import re


class JoinKeyStore:
    def __init__(self):
        self.rules = {
            "customer": {
                "canonical_format": "digits_only",
                "patterns": [r"^CUST[-_]?0*(\d+)$", r"^0*(\d+)$"],
                "examples": ["1", "001", "CUST-001", "cust_0002"],
            }
        }

    def get_normalization_method(self, entity: str) -> dict:
        return self.rules.get(
            entity,
            {
                "canonical_format": "strip_and_lower",
                "patterns": [r"(.+)"],
                "examples": [],
            },
        )

    def normalize_value(self, entity: str, raw_value: object) -> str:
        if raw_value is None:
            return ""
        raw_text = str(raw_value).strip()
        rule = self.get_normalization_method(entity)
        if rule["canonical_format"] == "digits_only":
            for pattern in rule["patterns"]:
                match = re.match(pattern, raw_text, flags=re.IGNORECASE)
                if match:
                    return match.group(1)
            digits = "".join(ch for ch in raw_text if ch.isdigit())
            return digits or raw_text.lower()
        return raw_text.lower()
