"""
text_inventory.py

Inventory of fields that require structured extraction.
"""

from __future__ import annotations


class TextInventory:
    def __init__(self):
        self.inventory = {
            "mongodb": {
                "support_tickets": {
                    "text_fields": ["note"],
                    "suggested_schema": ["negative_sentiment", "urgent", "mentions_billing", "mentions_delivery"],
                }
            }
        }

    def find_relevant_fields(
        self,
        required_sources: list[str],
        entities: list[str],
        needs_text_extraction: bool,
    ) -> dict:
        if not needs_text_extraction:
            return {}
        return {source: self.inventory.get(source, {}) for source in required_sources}
