"""
Multi-Pass Retrieval Utility

Retrieves context from the Knowledge Base in priority order:
  Pass 1: Schema & metadata (kb/domain/dab_schema.md)
  Pass 2: Domain knowledge (kb/domain/domain_terms.md, join_keys.md, unstructured_fields.md)
  Pass 3: Corrections log (kb/corrections/corrections_log.md)

Assembles a minimal context payload for the agent's working memory,
respecting a token budget to avoid context overflow.
"""

import os
import re
from pathlib import Path
from typing import Any


# Default KB file priority mapping
KB_PASSES = [
    {
        "pass_name": "schema",
        "description": "Schema and metadata context",
        "files": ["domain/dab_schema.md"],
    },
    {
        "pass_name": "domain",
        "description": "Domain knowledge, join keys, unstructured field inventory",
        "files": [
            "domain/domain_terms.md",
            "domain/join_keys.md",
            "domain/unstructured_fields.md",
        ],
    },
    {
        "pass_name": "corrections",
        "description": "Agent failure corrections for self-learning",
        "files": ["corrections/corrections_log.md"],
    },
    {
        "pass_name": "architecture",
        "description": "Architecture rules and patterns",
        "files": [
            "architecture/claude_code_memory.md",
            "architecture/openai_data_agent.md",
        ],
    },
]


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 characters per token for English text."""
    return len(text) // 4


def load_kb_file(kb_path: str, relative_file: str) -> dict[str, str]:
    """
    Load a single KB file and return its content with metadata.

    Args:
        kb_path: Root path to the kb/ directory
        relative_file: Relative path within kb/ (e.g., "domain/join_keys.md")

    Returns:
        {"path": str, "content": str, "tokens": int} or empty dict if file not found
    """
    full_path = os.path.join(kb_path, relative_file)
    if not os.path.exists(full_path):
        return {}

    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read().strip()

    return {
        "path": relative_file,
        "content": content,
        "tokens": estimate_tokens(content),
    }


def search_kb_for_terms(content: str, question: str) -> float:
    """
    Score how relevant a KB document is to a given question.
    Simple keyword overlap relevance scorer.

    Args:
        content: The KB document text
        question: The user's question

    Returns:
        Relevance score between 0.0 and 1.0
    """
    # Tokenize both into lowercase words
    question_words = set(re.findall(r"\b\w{3,}\b", question.lower()))
    content_words = set(re.findall(r"\b\w{3,}\b", content.lower()))

    if not question_words:
        return 0.0

    overlap = question_words & content_words
    return len(overlap) / len(question_words)


def retrieve_context(
    question: str,
    kb_path: str,
    max_tokens: int = 4000,
    relevance_threshold: float = 0.1,
    passes: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Retrieve context from the KB in priority order, respecting a token budget.

    The retrieval is multi-pass:
    1. Schema context (always included — foundational)
    2. Domain knowledge (included if relevant to the question)
    3. Corrections log (always included — self-learning loop)
    4. Architecture (included only if budget allows and relevant)

    Args:
        question: The user's natural language question
        kb_path: Path to the kb/ root directory
        max_tokens: Maximum token budget for the assembled context
        relevance_threshold: Minimum relevance score to include a non-mandatory file
        passes: Custom pass definitions (defaults to KB_PASSES)

    Returns:
        {
            "context_text": str,        # Assembled context for injection
            "sources_used": list[str],   # Which KB files were included
            "sources_skipped": list[str], # Which were skipped (budget or relevance)
            "total_tokens": int,         # Estimated token count of assembled context
            "passes_completed": int,     # How many passes fit in budget
        }
    """
    if passes is None:
        passes = KB_PASSES

    assembled_parts: list[str] = []
    sources_used: list[str] = []
    sources_skipped: list[str] = []
    total_tokens = 0

    for pass_def in passes:
        pass_name = pass_def["pass_name"]

        for relative_file in pass_def["files"]:
            file_data = load_kb_file(kb_path, relative_file)
            if not file_data:
                sources_skipped.append(f"{relative_file} (not found)")
                continue

            # Check relevance for non-critical passes
            is_critical = pass_name in ("schema", "corrections")
            if not is_critical:
                relevance = search_kb_for_terms(file_data["content"], question)
                if relevance < relevance_threshold:
                    sources_skipped.append(f"{relative_file} (relevance={relevance:.2f})")
                    continue

            # Check token budget
            if total_tokens + file_data["tokens"] > max_tokens:
                sources_skipped.append(f"{relative_file} (budget exceeded)")
                continue

            assembled_parts.append(f"--- {relative_file} ---\n{file_data['content']}")
            sources_used.append(relative_file)
            total_tokens += file_data["tokens"]

    passes_completed = len(set(
        pass_def["pass_name"]
        for pass_def in passes
        for f in pass_def["files"]
        if f in sources_used
    ))

    return {
        "context_text": "\n\n".join(assembled_parts),
        "sources_used": sources_used,
        "sources_skipped": sources_skipped,
        "total_tokens": total_tokens,
        "passes_completed": passes_completed,
    }
