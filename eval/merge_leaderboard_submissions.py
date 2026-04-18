"""Merge multiple flattened leaderboard JSON files into one consolidated file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge flattened leaderboard files.")
    parser.add_argument("--inputs", nargs="+", required=True, help="One or more flat leaderboard JSON files.")
    parser.add_argument("--output", required=True, help="Path to the merged leaderboard JSON file.")
    args = parser.parse_args()

    merged: list[dict[str, Any]] = []
    for input_path in args.inputs:
        rows = json.loads(Path(input_path).read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise TypeError(f"Expected a JSON array in {input_path}")
        merged.extend(rows)

    merged.sort(key=lambda row: (row.get("dataset", ""), str(row.get("query", "")), int(row.get("run", 0))))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print(json.dumps({"inputs": args.inputs, "output": str(output_path), "rows": len(merged)}, indent=2))


if __name__ == "__main__":
    main()
