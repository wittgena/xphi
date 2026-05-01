# xor.context.graph
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Union

@dataclass
class ContextGraph:
    """그래프 탐색 및 프로젝션에 사용되는 컨텍스트 모델"""
    entry: str
    focus: str = "CLI Observation"
    depth: int = 1
    relations: List[str] = field(default_factory=lambda: ["coupled"])

    @property
    def valid_relations(self) -> set:
        return set(self.relations)

@dataclass
class SurfaceTemplateData:
    """마크다운 렌더링에 주입되는 템플릿 데이터 모델"""
    entry_point: str
    focus: str
    depth: str
    relations_list: str
    fragments: str
    relations: str

def _extract_rel_attr(rel: Any, key: str, default: Any = None) -> Any:
    """dict 또는 객체 형태의 relation에서 속성을 안전하게 추출"""
    if isinstance(rel, dict):
        return rel.get(key, default)
    return getattr(rel, key, default)

class EntryTemplate:
    MARKDOWN = """# Entry: {entry_point}

> **Phase**: Φ′ → Ψ (Contextual Subgraph) → Φs
> **Role**: Dynamic Perspective Projection

## @focus.context
- **Focus**: `{focus}`
- **Depth**: {depth}
- **Relations**: {relations_list}

---

## @local.topology (Selected Fragments)
{fragments}

---

## @expansion (Paths)
{relations}

---
*Projected via `EntryProjector` (Dynamic Boundary)*
"""