"""Reusable key-normalization utilities used by cross-source joins."""

from __future__ import annotations


def yelp_business_id_to_ref(business_id: str) -> str:
    return business_id.replace("businessid_", "businessref_", 1)


def normalize_lower(value: str) -> str:
    return value.strip().lower()
