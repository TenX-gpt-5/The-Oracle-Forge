import sys
import os
import json

# Setup standard imports
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.agent.orchestrator import Orchestrator

def execute_benchmark_query():
    print("====================================")
    print("    FORGE AGENT V3 - DAB TEST RUN   ")
    print("====================================\n")
    
    agent = Orchestrator()
    
    sample_question = (
        "Which customer segments had order activity and how does that compare "
        "with support ticket volume across our sources?"
    )
    
    print(f"User Query: {sample_question}\n")
    print("Routing Trace Logic...")
    
    result = agent.execute_turn(sample_question)
    
    # Formatted terminal output for debugs
    print("\n[PLAN]")
    print(json.dumps(result.get("plan"), indent=2))
    
    print("\n[CONTEXT]")
    print(json.dumps(result.get("retrieved_context"), indent=2))

    print("\n[VALIDATION]")
    print(json.dumps(result.get("validation"), indent=2))

    print("\n[FINAL OUTPUT]")
    print(result.get("final_answer"))

    print("\n[PROMOTED MEMORIES]")
    print(json.dumps(result.get("promoted_memories"), indent=2))
    
    print("\n[SYSTEM TRACE]")
    for node in result.get("trace", []):
        print(f" -> {node['step'].upper()}: {node['action']}")

if __name__ == "__main__":
    execute_benchmark_query()
