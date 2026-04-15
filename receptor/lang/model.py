# receptor.lang.model
from typing import TypedDict, List, Dict, Any
from dataclasses import dataclass, asdict

class LangModel(TypedDict):
    total_modules: int
    total_dependencies: int
    cycles_detected: int
    layer_distribution: Dict[str, int]
    absorbable_count: int      # 즉시 분리/재사용 가능한 독립 모듈 수
    phase_stability: float     # 위상 안정성 점수 (100점 만점)

class NodeData(TypedDict):
    id: str
    file_path: str
    layer: str                 # L0 ~ L4
    is_topos: bool             # 내부 런타임/루프 존재 여부
    degree: int
    betweenness: float

class EdgeData(TypedDict):
    source: str
    target: str
    keyword: str
    linenos: List[int]

class LoopEdgeData(TypedDict):
    source: str
    target: str
    type: str

@dataclass
class PhaseGraphSchema:
    meta: LangModel
    nodes: List[NodeData]
    topos_edges: List[EdgeData]
    loop_edges: List[LoopEdgeData]
    cycles: List[List[str]]