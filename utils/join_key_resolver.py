"""
Join Key Resolver Utility

Detects format mismatches between join keys across databases and normalizes
them to a canonical form. Directly addresses DAB's "ill-formatted join keys"
hard requirement.
"""

import re
from typing import Any


# Known format patterns and their regex signatures
# Order matters: more specific patterns must come before general ones
FORMAT_PATTERNS = {
    "prefixed_integer": re.compile(r"^[A-Z]+-\d+$"),           # CUST-12345
    "uuid": re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE),
    "phone_dashed": re.compile(r"^\d{3}-\d{3}-\d{4}$"),         # 123-456-7890
    "phone_e164": re.compile(r"^\+\d{11,15}$"),                  # +11234567890
    "zero_padded_integer": re.compile(r"^0+\d+$"),               # 00456 (must be before integer)
    "integer": re.compile(r"^\d+$"),                             # 12345
    "hash_hex": re.compile(r"^[0-9a-f]{8,64}$", re.IGNORECASE), # truncated hash
}


def detect_format(samples: list[str]) -> str:
    """
    Detect the format of a list of key samples.

    Args:
        samples: List of string key values to analyze

    Returns:
        Format name string (e.g., "prefixed_integer", "integer", "uuid")
        Returns "unknown" if no pattern matches a majority of samples.
    """
    if not samples:
        return "unknown"

    format_votes: dict[str, int] = {}
    for sample in samples:
        sample_str = str(sample).strip()
        for fmt_name, pattern in FORMAT_PATTERNS.items():
            if pattern.match(sample_str):
                format_votes[fmt_name] = format_votes.get(fmt_name, 0) + 1
                break

    if not format_votes:
        return "unknown"

    best_format = max(format_votes, key=format_votes.get)
    # Require majority match
    if format_votes[best_format] >= len(samples) * 0.5:
        return best_format
    return "unknown"


def normalize_key(key: str, source_format: str, target_format: str = "string") -> Any:
    """
    Normalize a key from its source format to the target canonical form.

    Args:
        key: The raw key value
        source_format: Detected source format
        target_format: Desired output format ("integer", "string", "prefixed_integer")

    Returns:
        Normalized key in the target format
    """
    # Step 1: Extract the core value
    core_value = _extract_core(key, source_format)

    # Step 2: Convert to target format
    return _to_target(core_value, target_format)


def _extract_core(key: str, source_format: str) -> str:
    """Extract the core identifier value from a formatted key."""
    key = str(key).strip()

    if source_format == "prefixed_integer":
        # "CUST-12345" → "12345"
        match = re.match(r"^[A-Za-z]+-(.+)$", key)
        return match.group(1) if match else key

    elif source_format == "zero_padded_integer":
        # "00456" → "456"
        return str(int(key))

    elif source_format == "phone_dashed":
        # "123-456-7890" → "1234567890"
        return key.replace("-", "")

    elif source_format == "phone_e164":
        # "+11234567890" → "1234567890" (strip country code for comparison)
        return key.lstrip("+")[1:] if key.startswith("+1") else key.lstrip("+")

    return key


def _to_target(core_value: str, target_format: str) -> Any:
    """Convert a core value to a target format."""
    if target_format == "integer":
        try:
            return int(core_value)
        except ValueError:
            return core_value

    elif target_format == "prefixed_integer":
        try:
            return f"CUST-{int(core_value)}"
        except ValueError:
            return f"CUST-{core_value}"

    elif target_format == "zero_padded":
        try:
            return f"{int(core_value):05d}"
        except ValueError:
            return core_value

    elif target_format == "phone_e164":
        digits = re.sub(r"\D", "", core_value)
        if len(digits) == 10:
            return f"+1{digits}"
        return f"+{digits}"

    # Default: return as string
    return str(core_value)


def validate_overlap(left_keys: list, right_keys: list) -> dict[str, Any]:
    """
    Validate the overlap between two sets of join keys before performing a join.

    This MUST be called before any cross-database join to catch silent failures.

    Args:
        left_keys: Keys from the left/source table
        right_keys: Keys from the right/target table

    Returns:
        {
            "matched": int,         # number of keys present in both
            "left_only": int,       # keys only in left
            "right_only": int,      # keys only in right
            "overlap_pct": float,   # matched / total unique keys
            "warning": str | None   # warning if overlap is suspiciously low
        }
    """
    left_set = set(str(k) for k in left_keys)
    right_set = set(str(k) for k in right_keys)

    matched = left_set & right_set
    left_only = left_set - right_set
    right_only = right_set - left_set
    total = len(left_set | right_set)

    overlap_pct = len(matched) / total if total > 0 else 0.0

    warning = None
    if overlap_pct < 0.1:
        warning = f"CRITICAL: Only {overlap_pct:.1%} overlap. Likely format mismatch — normalize keys before joining."
    elif overlap_pct < 0.3:
        warning = f"LOW: {overlap_pct:.1%} overlap. Check for partial format mismatch or data quality issues."

    return {
        "matched": len(matched),
        "left_only": len(left_only),
        "right_only": len(right_only),
        "overlap_pct": round(overlap_pct, 4),
        "warning": warning,
    }


def resolve_and_normalize(
    left_keys: list[str],
    right_keys: list[str],
) -> dict[str, Any]:
    """
    End-to-end join key resolution: detect formats, normalize both sides, validate overlap.

    Args:
        left_keys: Raw keys from source A
        right_keys: Raw keys from source B

    Returns:
        {
            "left_format": str,
            "right_format": str,
            "normalized_left": list,
            "normalized_right": list,
            "overlap": dict,
            "recommendation": str
        }
    """
    left_fmt = detect_format(left_keys)
    right_fmt = detect_format(right_keys)

    # Normalize both to string for comparison
    norm_left = [normalize_key(k, left_fmt, "string") for k in left_keys]
    norm_right = [normalize_key(k, right_fmt, "string") for k in right_keys]

    overlap = validate_overlap(norm_left, norm_right)

    if overlap["overlap_pct"] > 0.5:
        recommendation = "Formats resolved. Safe to join on normalized keys."
    elif overlap["overlap_pct"] > 0.1:
        recommendation = "Partial overlap after normalization. Inspect sample mismatches manually."
    else:
        recommendation = "Very low overlap. Normalization may be incorrect or keys may not correspond."

    return {
        "left_format": left_fmt,
        "right_format": right_fmt,
        "normalized_left": norm_left,
        "normalized_right": norm_right,
        "overlap": overlap,
        "recommendation": recommendation,
    }
