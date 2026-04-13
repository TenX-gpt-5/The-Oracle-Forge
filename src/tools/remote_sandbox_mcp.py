"""
remote_sandbox_mcp.py

FastMCP server exposing remote sandbox tools for the DataAgentBench checkout.
This module is optional at runtime; it degrades gracefully when the `mcp`
package is not installed.
"""

from __future__ import annotations

from typing import Any

from src.tools.remote_sandbox import RemoteSandboxClient

try:  # pragma: no cover - optional dependency in some envs
    from mcp.server.fastmcp import FastMCP
except Exception:  # pragma: no cover
    FastMCP = None


client = RemoteSandboxClient()
mcp_server = FastMCP("OracleForge_RemoteSandbox") if FastMCP is not None else None


def remote_list_repo_root() -> dict[str, Any]:
    return client.list_repo_root()


def remote_run_command(command: str, cwd: str | None = None) -> dict[str, Any]:
    return client.run_command(command=command, cwd=cwd)


def remote_run_python(python_code: str, cwd: str | None = None) -> dict[str, Any]:
    return client.run_python(python_code=python_code, cwd=cwd)


def remote_verify_dab_checkout() -> dict[str, Any]:
    return client.verify_dab_checkout()


if mcp_server is not None:  # pragma: no branch
    mcp_server.tool()(remote_list_repo_root)
    mcp_server.tool()(remote_run_command)
    mcp_server.tool()(remote_run_python)
    mcp_server.tool()(remote_verify_dab_checkout)


def serve() -> None:
    if mcp_server is None:  # pragma: no cover
        raise RuntimeError("FastMCP is not installed in this Python environment.")
    mcp_server.run()


if __name__ == "__main__":  # pragma: no cover
    serve()
