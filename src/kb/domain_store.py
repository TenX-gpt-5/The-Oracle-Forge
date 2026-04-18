"""
domain_store.py

Resolves domain terms using project memory plus curated fallbacks.
"""

from __future__ import annotations

from src.kb.project_memory import ProjectMemory


class DomainStore:
    def __init__(self, project_memory: ProjectMemory):
        self.project_memory = project_memory
        self.fallbacks = {
            "repeat_purchase_rate": "Count customers with at least two completed orders divided by the total customers in scope.",
            "active_user": "A user active in the last 30 days unless the query defines a different window.",
            "revenue": "Revenue comes from completed orders only.",
            "support_ticket_volume": "Support ticket volume is the ticket count over the requested period.",
            "agnews_category": (
                "AG News articles have no stored category field. "
                "Classify by matching title and description text: "
                "Business = finance, economy, market, stock, company, corporate, trade, earnings, profit, CEO, merger, acquisition; "
                "Sports = NFL, NBA, MLB, FIFA, tournament, championship, athlete, coach, game, match, score; "
                "World = politics, government, war, election, president, minister, treaty, UN, military; "
                "Science/Technology = research, science, technology, software, AI, space, climate, medical, study."
            ),
        }

    def resolve_terms(self, terms: list[str]) -> dict:
        resolved = self.project_memory.get_domain_definitions(terms)
        for term in terms:
            if term not in resolved and term in self.fallbacks:
                resolved[term] = self.fallbacks[term]
        return resolved
