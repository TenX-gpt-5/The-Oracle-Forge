"""
mcp_stdio_client.py

Thin synchronous wrapper over the official MCP Python stdio client.
Used to connect to config-defined database MCP servers such as DuckDB.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.types import Implementation


class MCPStdIOClient:
    def __init__(self, config_path: str, server_name: str) -> None:
        self.config_path = Path(config_path)
        self.server_name = server_name

    def configured(self) -> bool:
        return self.config_path.exists()

    def available(self) -> bool:
        if not self.configured():
            return False
        spec = self._server_spec()
        if not spec:
            return False
        return shutil.which(spec["command"]) is not None or Path(spec["command"]).exists()

    def list_tools(self) -> list[dict[str, Any]]:
        return anyio.run(self._list_tools_async)

    def call_first_matching_tool(
        self,
        preferred_names: list[str],
        argument_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return anyio.run(self._call_first_matching_tool_async, preferred_names, argument_candidates)

    async def _list_tools_async(self) -> list[dict[str, Any]]:
        async with self._session() as session:
            result = await session.list_tools()
        return [tool.model_dump() if hasattr(tool, "model_dump") else dict(tool) for tool in result.tools]

    async def _call_first_matching_tool_async(
        self,
        preferred_names: list[str],
        argument_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        tools = await self._list_tools_async()
        normalized = {tool.get("name", ""): tool for tool in tools if isinstance(tool, dict)}

        ordered_names: list[str] = []
        for preferred_name in preferred_names:
            if preferred_name in normalized:
                ordered_names.append(preferred_name)
        for tool_name in normalized:
            if tool_name not in ordered_names:
                ordered_names.append(tool_name)

        last_error: str | None = None
        async with self._session() as session:
            for tool_name in ordered_names:
                for args in argument_candidates:
                    try:
                        response = await session.call_tool(tool_name, args)
                    except Exception as exc:
                        last_error = str(exc)
                        continue
                    parsed = _parse_mcp_tool_result(response)
                    if parsed["ok"]:
                        parsed["tool_name"] = tool_name
                        parsed["arguments"] = args
                        return parsed
                    last_error = parsed.get("error")

        return {"ok": False, "error": last_error or "No MCP tool call succeeded."}

    def _server_spec(self) -> dict[str, Any] | None:
        payload = json.loads(self.config_path.read_text())
        servers = payload.get("servers", {})
        spec = servers.get(self.server_name)
        return spec if isinstance(spec, dict) else None

    def _build_env(self, env_patch: dict[str, str] | None) -> dict[str, str]:
        env = os.environ.copy()
        if env_patch:
            env.update(env_patch)

        spec = self._server_spec() or {}
        command = spec.get("command", "")
        if Path(command).name == "uvx":
            forge_home = "/tmp/oracle-forge-mcp-home"
            Path(forge_home).mkdir(parents=True, exist_ok=True)
            env.setdefault("HOME", forge_home)
            env.setdefault("UV_CACHE_DIR", "/tmp/oracle-forge-uv-cache")
            env.setdefault("XDG_CACHE_HOME", "/tmp/oracle-forge-xdg-cache")
            env.setdefault("UV_TOOL_DIR", "/tmp/oracle-forge-uv-tools")
            Path(env["UV_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
            Path(env["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)
            Path(env["UV_TOOL_DIR"]).mkdir(parents=True, exist_ok=True)

        return env

    def _session(self):
        spec = self._server_spec()
        if not spec:
            raise RuntimeError(f"MCP server '{self.server_name}' not found in {self.config_path}")
        server = StdioServerParameters(
            command=spec["command"],
            args=spec.get("args", []),
            env=self._build_env(spec.get("env")),
        )
        return _MCPSessionContext(server)


class _MCPSessionContext:
    def __init__(self, server: StdioServerParameters) -> None:
        self.server = server
        self._transport = None
        self._session = None

    async def __aenter__(self) -> ClientSession:
        self._transport = stdio_client(self.server)
        read_stream, write_stream = await self._transport.__aenter__()
        self._session = ClientSession(
            read_stream,
            write_stream,
            client_info=Implementation(name="oracle-forge", version="0.1.0"),
        )
        await self._session.__aenter__()
        await self._session.initialize()
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.__aexit__(exc_type, exc, tb)
        if self._transport is not None:
            await self._transport.__aexit__(exc_type, exc, tb)


def _parse_mcp_tool_result(response: Any) -> dict[str, Any]:
    structured = response.model_dump() if hasattr(response, "model_dump") else response
    content = structured.get("content", [])
    if not isinstance(content, list):
        return {"ok": False, "error": f"Unexpected MCP content payload: {structured}"}

    parsed_items: list[Any] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text":
            parsed_items.append(_try_json(item.get("text", "")))
        elif item.get("type") == "json":
            parsed_items.append(item.get("json"))
        elif "text" in item:
            parsed_items.append(_try_json(str(item["text"])))

    if not parsed_items:
        return {"ok": False, "error": f"No usable MCP tool content: {structured}"}
    if len(parsed_items) == 1:
        return {"ok": True, "payload": parsed_items[0]}
    return {"ok": True, "payload": parsed_items}


def _try_json(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value
