# xphi.arch.xor.parser.mcp
import json
import logging
from typing import Dict, Any, Optional

log = logging.getLogger("parser.mcp")

MCP_EXTRACTION_RULES = {
    "text_content_path": "result.content.0.text"
}

class McpStateTraverser:
    """혼합된 데이터 토폴로지(Dict, List, Object)를 지정된 경로(Path)로 안전하게 탐색합니다."""
    @staticmethod
    def resolve(obj: Any, path: str, default: Any = None) -> Any:
        if not path or obj is None:
            return default

        keys = path.split('.')
        current = obj
        for k in keys:
            if current is None:
                return default
            if isinstance(current, dict):
                current = current.get(k)
            elif isinstance(current, (list, tuple)):
                try:
                    current = current[int(k)]
                except (IndexError, ValueError):
                    return default
            else:
                current = getattr(current, k, None)
        return current if current is not None else default

class McpPayloadParser:
    @staticmethod
    def extract_telemetry(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not payload or not isinstance(payload, dict):
            return None

        try:
            path = MCP_EXTRACTION_RULES["text_content_path"]
            text_data = McpStateTraverser.resolve(payload, path, "")
            if not text_data or not isinstance(text_data, str):
                return None

            parsed_data = json.loads(text_data)
            if isinstance(parsed_data, dict):
                return parsed_data.get("telemetry")
        except json.JSONDecodeError:
            pass
        except Exception as e:
            log.warning(f"[McpParser] Payload extraction failed: {e}")
            
        return None