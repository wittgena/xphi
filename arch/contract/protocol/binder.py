# xphi.arch.contract.protocol.binder
from __future__ import annotations

import inspect
import json
import re
from typing import Any, Callable, Dict, List, Literal, Union, get_args, get_origin, get_type_hints

_PARAM_DOC_RE = re.compile(
    r"^(?:[:@]param\s+|-\s*|\*\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\([^)]*\))?\s*:\s*(.+)$"
)


class ToolError(Exception):
    """MCP 콘텐츠 레벨 에러 (isError: True 반환용)"""
    pass


def _parse_docstring(doc: str) -> tuple[str, Dict[str, str]]:
    """독스트링에서 (도구 요약 설명, {파라미터명: 설명})을 자동 추출"""
    if not doc:
        return "", {}

    summary_lines: List[str] = []
    param_descs: Dict[str, str] = {}
    in_args_block = False

    for raw_line in doc.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.lower() in ("args:", "arguments:", "parameters:", "params:"):
            in_args_block = True
            continue

        match = _PARAM_DOC_RE.match(line)
        if match and (in_args_block or line.startswith((":", "@", "-", "*"))):
            param_descs[match.group(1)] = match.group(2).strip()
        elif not in_args_block and not param_descs:
            summary_lines.append(line)

    return " ".join(summary_lines), param_descs


def _py_type_to_json_schema(py_type: Any) -> Dict[str, Any]:
    """파이썬 타입 힌트를 JSON Schema 타입으로 변환"""
    if py_type is inspect.Parameter.empty or py_type is Any:
        return {"type": "string"}

    origin = get_origin(py_type)
    args = get_args(py_type)

    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        return _py_type_to_json_schema(non_none[0]) if len(non_none) == 1 else {"type": "string"}

    if origin is Literal:
        return {"type": "string", "enum": list(args)}

    if origin in (list, List):
        return {"type": "array", "items": _py_type_to_json_schema(args[0] if args else Any)}

    if origin in (dict, Dict) or py_type is dict:
        return {"type": "object"}

    return dict({
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
    }.get(py_type, {"type": "string"}))


class ToolBinderMixin:
    """`tool_<name>` 메서드의 시그니처와 독스트링만으로 MCP 스키마/디스패치를 자동 완성하는 Mixin"""

    def _init_tool_binder(self) -> None:
        self._bound_tools: Dict[str, Callable] = {}
        self._tool_schemas: List[Dict[str, Any]] = []

        for attr_name in dir(self):
            if not attr_name.startswith("tool_"):
                continue
            member = getattr(self, attr_name, None)
            if callable(member):
                tool_name = attr_name[5:]
                self._bound_tools[tool_name] = member
                self._tool_schemas.append(self._build_tool_schema(tool_name, member))

    def _build_tool_schema(self, tool_name: str, func: Callable) -> Dict[str, Any]:
        sig = inspect.signature(func)
        try:
            type_hints = get_type_hints(func)
        except Exception:
            type_hints = {}

        summary, param_descs = _parse_docstring(inspect.getdoc(func) or "")
        properties: Dict[str, Any] = {}
        required: List[str] = []

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "meta", "_meta"):
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue

            prop_schema = _py_type_to_json_schema(type_hints.get(param_name, param.annotation))

            if param.default is inspect.Parameter.empty:
                required.append(param_name)
            elif param.default is not None:
                prop_schema["default"] = param.default

            if param_name in param_descs:
                prop_schema["description"] = param_descs[param_name]

            properties[param_name] = prop_schema

        schema_entry: Dict[str, Any] = {
            "name": tool_name,
            "inputSchema": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        }
        if summary:
            schema_entry["description"] = summary

        return schema_entry

    def _prepare_call_kwargs(
        self, func: Callable, arguments: Dict[str, Any], meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        sig = inspect.signature(func)
        try:
            type_hints = get_type_hints(func)
        except Exception:
            type_hints = {}

        call_kwargs: Dict[str, Any] = {}
        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue
            if param_name in ("meta", "_meta"):
                call_kwargs[param_name] = meta
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue

            if param_name in arguments:
                val = arguments[param_name]
                hint = type_hints.get(param_name, param.annotation)
                if hint is int and val is not None and not isinstance(val, bool):
                    val = int(val)
                elif hint is float and val is not None and not isinstance(val, bool):
                    val = float(val)
                elif hint is str and val is not None:
                    val = str(val)
                call_kwargs[param_name] = val
            elif param.default is not inspect.Parameter.empty:
                call_kwargs[param_name] = param.default
            else:
                raise ValueError(f"Missing required parameter: '{param_name}'")

        return call_kwargs

    @staticmethod
    def _wrap_mcp_result(result: Any, is_error: bool = False) -> Dict[str, Any]:
        if isinstance(result, dict) and "content" in result and isinstance(result["content"], list):
            return result
        text = result if isinstance(result, str) else json.dumps(result)
        return {"content": [{"type": "text", "text": text}], "isError": is_error}