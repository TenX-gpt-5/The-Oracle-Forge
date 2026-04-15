"""
Reusable key-normalization utilities used by cross-source joins.

# Usage Example:
#   from utils.key_normalization import yelp_business_id_to_ref
#   ref = yelp_business_id_to_ref("businessid_123")
#   print(ref) # Returns "businessref_123"
"""

from __future__ import annotations


def yelp_business_id_to_ref(business_id: str) -> str:
    return business_id.replace("businessid_", "businessref_", 1)


def normalize_lower(value: str) -> str:
    return value.strip().lower()
