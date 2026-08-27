# xphi.watcher.tracer.scope
## @lineage: watcher.tracer.scope
import contextvars
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Tuple

@dataclass(frozen=True)
class ScopeNode:
    """단일 스코프 노드의 메타데이터"""
    name: str
    facet: str  # 예: 'infra', 'logical', 'execution'
    metadata: dict[str, Any] = field(default_factory=dict)
    entered_at: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

_scope_stack: contextvars.ContextVar[Tuple[ScopeNode, ...]] = contextvars.ContextVar(
    "scope_stack", default=()
)

class scope_trace:
    """스코프 진입 및 이탈을 추적하는 컨텍스트 매니저 (동기/비동기 모두 지원)"""
    def __init__(self, name: str, facet: str, **metadata):
        self.node = ScopeNode(name=name, facet=facet, metadata=metadata)
        self._token = None

    def __enter__(self):
        # 현재 스택을 가져와 새로운 노드를 추가
        current_stack = _scope_stack.get()
        new_stack = current_stack + (self.node,)
        self._token = _scope_stack.set(new_stack)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._token is not None:
            _scope_stack.reset(self._token)

    async def __aenter__(self):
        """비동기 컨텍스트 진입 (동기 메서드 위임)"""
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 이탈 (동기 메서드 위임)"""
        return self.__exit__(exc_type, exc_val, exc_tb)

def get_current_trace_path() -> str:
    stack = _scope_stack.get()
    if not stack:
        return "[root]"
    return " -> ".join([f"[{node.facet}:{node.name}]" for node in stack])

def get_active_metadata(key: str) -> Any:
    """스코프 스택을 역순(최신순)으로 탐색하며 특정 메타데이터 값을 찾음"""
    stack = _scope_stack.get()
    for node in reversed(stack):
        if key in node.metadata:
            return node.metadata[key]
    return None