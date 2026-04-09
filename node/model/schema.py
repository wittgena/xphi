# node.model.schema
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Set, Optional

@dataclass
class ToposRelation:
    """노드 간의 결합(Edge) 상태"""
    target: str
    rel: str = "coupled"  # 관계 타입 (coupled, flows_into 등)
    strength: int = 1

@dataclass
class ToposNode:
    """매니폴드 내의 단일 위상 노드"""
    id: str
    intensity: int
    is_invariant: bool
    boundaries: Dict[str, int]
    support_manifold: List[str]
    relations: List[ToposRelation] = field(default_factory=list)

@dataclass
class ToposGraph:
    """전체 시스템의 위상 지도 (Model Manifold Projection)"""
    invariants: List[str]
    nodes: Dict[str, ToposNode]
    version: str = "2.1"
    type: str = "topos.network"

    def to_dict(self) -> dict:
        return asdict(self)