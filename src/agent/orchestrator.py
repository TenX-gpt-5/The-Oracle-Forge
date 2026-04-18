"""
orchestrator.py

The executive layer of Oracle Forge.
Coordinates planning, context retrieval, scratchpad creation, execution,
validation, repair, synthesis, and learning promotion.
"""

from __future__ import annotations

from typing import Any, Dict

from src.agent.execution_router import ExecutionRouter
from src.agent.repair_loop import RepairLoop
from src.agent.scratchpad_manager import ScratchpadManager
from src.agent.synthesizer import AnswerSynthesizer
from src.agent.validator import Validator
from src.eval.trace_logger import TraceLogger
from src.kb.context_cortex import ContextCortex
from src.kb.global_memory import GlobalMemory
from src.kb.project_memory import ProjectMemory
from src.memory.consolidator import Consolidator
from src.memory.experience_store import ExperienceStore
from src.memory.knowledge_review import KnowledgeReview
from src.planning.planner import Planner
from src.tools.remote_sandbox import RemoteSandboxConfig


class Orchestrator:
    def __init__(
        self,
        max_retries: int = 2,
        remote_config: RemoteSandboxConfig | None = None,
    ):
        self.max_retries = max_retries

        self.global_memory = GlobalMemory()
        self.project_memory = ProjectMemory()
        self.experience_store = ExperienceStore()

        self.planner_module = Planner()
        self.cortex_module = ContextCortex(
            global_memory=self.global_memory,
            project_memory=self.project_memory,
            experience_store=self.experience_store,
        )
        self.scratchpad_manager = ScratchpadManager()
        self.router_module = ExecutionRouter(remote_config=remote_config)
        self.validator_module = Validator()
        self.repair_module = RepairLoop()
        self.synthesizer = AnswerSynthesizer()
        self.logger = TraceLogger(self.experience_store)
        self.consolidator = Consolidator()
        self.review_gate = KnowledgeReview(
            global_memory=self.global_memory,
            project_memory=self.project_memory,
        )

    def _append_trace(
        self,
        trace: list[dict[str, Any]],
        step: str,
        action: str,
        result: dict[str, Any],
    ) -> None:
        trace.append({"step": step, "action": action, "result": result})

    def execute_turn(
        self,
        user_question: str,
        benchmark_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Handle a single analytical turn end to end.
        """
        state: Dict[str, Any] = {
            "question": user_question,
            "trace": [],
            "retries": 0,
            "repair_context": {},
            "benchmark_context": benchmark_context or {},
        }

        validation: dict[str, Any] = {"status": "failed", "errors": ["No validation run"]}
        execution_result: dict[str, Any] = {}
        context_payload: dict[str, Any] = {}
        plan_result: dict[str, Any] = {}
        scratchpads: list[dict[str, Any]] = []
        remote_validation: dict[str, Any] | None = None

        while True:
            plan_result = self.planner_module.generate_plan(
                user_question,
                repair_context=state["repair_context"],
                benchmark_context=state["benchmark_context"],
            )
            self._append_trace(state["trace"], "planner", "generated_plan", plan_result)

            context_payload = self.cortex_module.retrieve_context(
                question=user_question,
                plan=plan_result,
                repair_context=state["repair_context"],
                benchmark_context=state["benchmark_context"],
            )
            self._append_trace(
                state["trace"],
                "context_cortex",
                "retrieved_context",
                context_payload,
            )

            scratchpads = self.scratchpad_manager.create_scratchpads(
                question=user_question,
                plan=plan_result,
                context=context_payload,
            )
            self._append_trace(
                state["trace"],
                "scratchpad_manager",
                "created_scratchpads",
                {"scratchpads": scratchpads},
            )

            execution_result = self.router_module.execute_plan(
                question=user_question,
                plan=plan_result,
                context_payload=context_payload,
                scratchpads=scratchpads,
                repair_context=state["repair_context"],
                benchmark_context=state["benchmark_context"],
            )
            self._append_trace(
                state["trace"],
                "execution_router",
                "executed_plan",
                execution_result,
            )

            validation = self.validator_module.validate_execution(
                question=user_question,
                plan=plan_result,
                execution_result=execution_result,
                context_payload=context_payload,
                benchmark_context=state["benchmark_context"],
            )
            self._append_trace(
                state["trace"],
                "validator",
                "validated_execution",
                validation,
            )

            if (
                validation["status"] == "passed"
                and state["benchmark_context"].get("dataset")
                and state["benchmark_context"].get("query_id")
            ):
                provisional_answer = self.synthesizer.synthesize(
                    question=user_question,
                    plan=plan_result,
                    context_payload=context_payload,
                    execution_result=execution_result,
                    validation=validation,
                )
                self._append_trace(
                    state["trace"],
                    "answer_synthesizer",
                    "generated_retry_answer",
                    {"provisional_answer": provisional_answer},
                )
                remote_validation = self.router_module.remote_dab.validate_answer(
                    dataset=state["benchmark_context"]["dataset"],
                    query_id=int(state["benchmark_context"]["query_id"]),
                    answer=provisional_answer,
                )
                self._append_trace(
                    state["trace"],
                    "benchmark_validator",
                    "validated_answer",
                    remote_validation,
                )
                if not remote_validation.get("is_valid", False):
                    remote_reason = str(remote_validation.get("reason", "Unknown remote validation failure"))
                    validation = {
                        "status": "failed",
                        "errors": validation.get("errors", [])
                        + [f"Remote DAB validator rejected answer: {remote_reason}"],
                        "failure_class": "benchmark_external_validation_failed",
                        "evidence": validation.get("evidence", []) + [f"remote_reason={remote_reason}"],
                    }
                    self._append_trace(
                        state["trace"],
                        "validator",
                        "validated_remote_benchmark",
                        validation,
                    )
                else:
                    validation["evidence"] = validation.get("evidence", []) + [
                        f"remote_validator={remote_validation.get('reason', 'OK')}"
                    ]

            if validation["status"] == "passed" or state["retries"] >= self.max_retries:
                break

            repair_payload = self.repair_module.handle_failure(
                {
                    "question": user_question,
                    "plan": plan_result,
                    "retrieved_context": context_payload,
                    "execution_result": execution_result,
                    "validation": validation,
                    "retries": state["retries"],
                    "trace": state["trace"],
                }
            )
            state["repair_context"] = repair_payload
            state["retries"] = repair_payload["retry_count"]
            self._append_trace(
                state["trace"],
                "repair_loop",
                "prepared_retry",
                repair_payload,
            )

        final_answer = self.synthesizer.synthesize(
            question=user_question,
            plan=plan_result,
            context_payload=context_payload,
            execution_result=execution_result,
            validation=validation,
        )
        self._append_trace(
            state["trace"],
            "answer_synthesizer",
            "generated_answer",
            {"final_answer": final_answer},
        )

        trace_record = {
            "question": user_question,
            "plan": plan_result,
            "retrieved_context": context_payload,
            "benchmark_context": state["benchmark_context"],
            "scratchpads": scratchpads,
            "execution_result": execution_result,
            "validation": validation,
            "remote_validation": remote_validation,
            "final_answer": final_answer,
            "tool_calls": execution_result.get("tool_calls", []),
            "trace": state["trace"],
            "retries": state["retries"],
            "success": validation["status"] == "passed",
        }
        benchmark_mode = bool(state["benchmark_context"].get("dataset"))
        if benchmark_mode:
            experience_id = -1
            promoted_memories = []
        else:
            try:
                experience_id = self.logger.log_trace(trace_record)
            except Exception as exc:  # pragma: no cover - persistence should not block benchmark runs
                experience_id = -1
                self._append_trace(
                    trace_record["trace"],
                    "trace_logger",
                    "log_trace_failed",
                    {"error": str(exc)},
                )

            candidate_memories = self.consolidator.consolidate_experiences([trace_record])
            promoted_memories = self.review_gate.review_and_promote(candidate_memories)

        return {
            "question": user_question,
            "plan": plan_result,
            "retrieved_context": context_payload,
            "scratchpads": scratchpads,
            "execution_result": execution_result,
            "validation": validation,
            "remote_validation": remote_validation,
            "final_answer": final_answer,
            "trace": state["trace"],
            "retries": state["retries"],
            "experience_id": experience_id,
            "promoted_memories": promoted_memories,
        }
