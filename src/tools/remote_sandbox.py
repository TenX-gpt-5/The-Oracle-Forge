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
from pathlib import Path
from typing import Any


DEFAULT_REMOTE_HOST = "trp-gpt5"
DEFAULT_REMOTE_DAB_PATH = "/shared/DataAgentBench"
DEFAULT_REMOTE_PYTHON = "/usr/bin/python3"
DEFAULT_REMOTE_CODE_PATH = "/shared/DataAgentBench/oracle_forge_v3"


@dataclass(frozen=True)
class RemoteSandboxConfig:
    host: str = os.getenv("REMOTE_SANDBOX_HOST", DEFAULT_REMOTE_HOST)
    dab_path: str = os.getenv("REMOTE_SANDBOX_DAB_PATH", DEFAULT_REMOTE_DAB_PATH)
    code_path: str = os.getenv("REMOTE_SANDBOX_CODE_PATH", DEFAULT_REMOTE_CODE_PATH)
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
            return self._run_local_command(command=remote_command, cwd=cwd)
        ssh_cmd = [
            "ssh",
            *self.config.ssh_options,
            self.config.host,
            remote_command,
        ]
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)
        if result.returncode != 0 and self._should_fallback_to_local(result):
            return self._run_local_command(command=remote_command, cwd=cwd, fallback_host=self.config.host)
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

    def _should_fallback_to_local(self, result: subprocess.CompletedProcess[str]) -> bool:
        stderr = str(result.stderr or "")
        stdout = str(result.stdout or "")
        combined = f"{stderr}\n{stdout}".lower()
        if not Path(self.config.dab_path).exists():
            return False
        if "could not resolve hostname" in combined:
            return True
        if "name or service not known" in combined:
            return True
        if "no route to host" in combined:
            return True
        return False

    def _run_local_command(
        self,
        command: str,
        cwd: str | None = None,
        fallback_host: str | None = None,
    ) -> dict[str, Any]:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=cwd or self.config.dab_path,
            shell=True,
        )
        payload = {
            "ok": result.returncode == 0,
            "command": command,
            "cwd": cwd or self.config.dab_path,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "returncode": result.returncode,
            "host": fallback_host or self.config.host,
            "transport": "local-fallback" if fallback_host else "local",
        }
        return payload

    def list_repo_root(self) -> dict[str, Any]:
        return self.run_command("pwd && ls -1")

    def run_python(self, python_code: str, cwd: str | None = None) -> dict[str, Any]:
        encoded = base64.b64encode(python_code.encode("utf-8")).decode("ascii")
        candidate_pythons = [self.config.python_executable, "python3"]
        last_response: dict[str, Any] | None = None
        seen: set[str] = set()
        for python_executable in candidate_pythons:
            if not python_executable or python_executable in seen:
                continue
            seen.add(python_executable)
            command = (
                f"{shlex.quote(python_executable)} - <<'PY'\n"
                "import base64\n"
                f"code = base64.b64decode('{encoded}').decode('utf-8')\n"
                "namespace = {}\n"
                "exec(code, namespace, namespace)\n"
                "PY"
            )
            response = self.run_command(command, cwd=cwd)
            if response.get("ok"):
                return response
            last_response = response
            stderr = str(response.get("stderr", ""))
            if "No such file or directory" not in stderr and "command not found" not in stderr:
                return response
        return last_response or self.run_command("python3 -c 'print(\"failed\")'", cwd=cwd)

    def verify_dab_checkout(self) -> dict[str, Any]:
        return self.run_command("pwd && test -f README.md && echo DAB_READY")
