"""
Reusable attribute parsing helpers for Yelp-like metadata flags.

# Usage Example:
#   from utils.attribute_flags import is_truthy
#   flag = is_truthy("u'no'")
#   print(flag) # Returns False
"""

from __future__ import annotations

import ast
from typing import Any


FALSE_TOKENS = {"", "none", "null", "false", "0", "u'no'", "no", "n"}


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in FALSE_TOKENS:
        return False
    return "true" in text or text in {"yes", "y", "u'free'", "u'paid'", "free", "paid"}


def supports_business_or_bike_parking(attributes: Any) -> bool:
    if not isinstance(attributes, dict):
        return False
    if is_truthy(attributes.get("BikeParking")):
        return True

    business_parking = attributes.get("BusinessParking")
    if isinstance(business_parking, dict):
        return any(bool(value) for value in business_parking.values())
    if isinstance(business_parking, str):
        try:
            parsed = ast.literal_eval(business_parking)
            if isinstance(parsed, dict):
                return any(bool(value) for value in parsed.values())
        except (ValueError, SyntaxError):
            return "true" in business_parking.lower()
    return False
