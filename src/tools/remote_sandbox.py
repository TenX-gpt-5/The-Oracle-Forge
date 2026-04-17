"""
remote_sandbox.py

Remote sandbox access for the DAB checkout via an MCP-friendly bridge.
The bridge uses SSH as transport and exposes structured methods that can be
called directly by the runtime or surfaced through a FastMCP server.
"""

from __future__ import annotations

import base64
import os
import shlex
import subprocess
from dataclasses import dataclass
from typing import Any


DEFAULT_REMOTE_HOST = "trp-gpt5"
DEFAULT_REMOTE_DAB_PATH = "/shared/DataAgentBench"
DEFAULT_REMOTE_PYTHON = "/shared/DataAgentBench/oracle_forge_v3/venv/bin/python"


@dataclass(frozen=True)
class RemoteSandboxConfig:
    host: str = os.getenv("REMOTE_SANDBOX_HOST", DEFAULT_REMOTE_HOST)
    dab_path: str = os.getenv("REMOTE_SANDBOX_DAB_PATH", DEFAULT_REMOTE_DAB_PATH)
    python_executable: str = os.getenv(
        "REMOTE_SANDBOX_PYTHON",
        DEFAULT_REMOTE_PYTHON,
    )
    ssh_options: tuple[str, ...] = (
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=8",
    )


class RemoteSandboxClient:
    def __init__(self, config: RemoteSandboxConfig | None = None):
        self.config = config or RemoteSandboxConfig()

    def enabled(self) -> bool:
        return os.getenv("REMOTE_SANDBOX_ENABLED", "false").lower() in {"1", "true", "yes"}

    def build_remote_command(self, command: str, cwd: str | None = None) -> str:
        target_dir = cwd or self.config.dab_path
        return f"cd {shlex.quote(target_dir)} && {command}"

    def run_command(self, command: str, cwd: str | None = None) -> dict[str, Any]:
        remote_command = self.build_remote_command(command, cwd=cwd)
        if self._is_local_host():
            result = subprocess.run(
                remote_command,
                capture_output=True,
                text=True,
                cwd=cwd or self.config.dab_path,
                shell=True,
            )
            return {
                "ok": result.returncode == 0,
                "command": command,
                "cwd": cwd or self.config.dab_path,
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "returncode": result.returncode,
                "host": self.config.host,
            }
        ssh_cmd = [
            "ssh",
            *self.config.ssh_options,
            self.config.host,
            remote_command,
        ]
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)
        return {
            "ok": result.returncode == 0,
            "command": command,
            "cwd": cwd or self.config.dab_path,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "returncode": result.returncode,
            "host": self.config.host,
        }

    def _is_local_host(self) -> bool:
        return self.config.host in {"localhost", "127.0.0.1", "::1"}

    def list_repo_root(self) -> dict[str, Any]:
        return self.run_command("pwd && ls -1")

    def run_python(self, python_code: str, cwd: str | None = None) -> dict[str, Any]:
        encoded = base64.b64encode(python_code.encode("utf-8")).decode("ascii")
        command = (
            f"{shlex.quote(self.config.python_executable)} - <<'PY'\n"
            "import base64\n"
            f"code = base64.b64decode('{encoded}').decode('utf-8')\n"
            "namespace = {}\n"
            "exec(code, namespace, namespace)\n"
            "PY"
        )
        return self.run_command(command, cwd=cwd)

    def verify_dab_checkout(self) -> dict[str, Any]:
        return self.run_command("pwd && test -f README.md && echo DAB_READY")
