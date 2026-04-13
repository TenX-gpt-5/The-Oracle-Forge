import argparse
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.agent.orchestrator import Orchestrator
from src.dab.remote_dab_adapter import RemoteDABAdapter


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Oracle Forge against a remote DAB query bundle.")
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. crmarenapro")
    parser.add_argument("--query-id", required=True, type=int, help="Query number within the dataset")
    parser.add_argument("--use-hints", action="store_true", help="Append db_description_withhint.txt")
    parser.add_argument("--validate-answer", action="store_true", help="Run remote validate.py on the final answer")
    args = parser.parse_args()

    os.environ.setdefault("REMOTE_SANDBOX_ENABLED", "true")

    adapter = RemoteDABAdapter()
    bundle = adapter.get_query_bundle(args.dataset, args.query_id, use_hints=args.use_hints)
    if not bundle or bundle.get("ok") is False and "query_text" not in bundle:
        raise RuntimeError(f"Failed to fetch remote query bundle: {bundle}")

    agent = Orchestrator()
    result = agent.execute_turn(bundle["query_text"], benchmark_context=bundle)

    print("\n[QUERY]")
    print(bundle["query_text"])

    print("\n[DB CLIENTS]")
    print(json.dumps(bundle["db_clients"], indent=2))

    print("\n[PLAN]")
    print(json.dumps(result["plan"], indent=2))

    print("\n[VALIDATION]")
    print(json.dumps(result["validation"], indent=2))

    print("\n[FINAL ANSWER]")
    print(result["final_answer"])

    if args.validate_answer:
        if result.get("validation", {}).get("status") == "passed":
            validation = adapter.validate_answer(args.dataset, args.query_id, result["final_answer"])
            print("\n[REMOTE DAB VALIDATE]")
            print(json.dumps(validation, indent=2))
        else:
            print("\n[REMOTE DAB VALIDATE]")
            print(
                json.dumps(
                    {
                        "skipped": True,
                        "reason": "Skipped because internal validation failed.",
                    },
                    indent=2,
                )
            )


if __name__ == "__main__":
    main()
