# ops.tester.state.schema
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from arch.gov.state.vocab import NodeType, EdgeMode

@dataclass
class AgentAttributes:
    """LLM 에이전트 루프 및 핸들러 제어를 위한 물리적 속성 정의"""
    instructions: Optional[str] = None       # 주입될 프롬프트/가이드라인
    pressure: float = 0.0                    # 시스템 압박도 (organizer에서 [URGENT] 트리거 등에 사용)
    max_failures: int = 3                    # 피로도(fatigue) 제어 및 위상 붕괴 방지용 루프 제한
    injection_role: str = "user"             # LLM 컨텍스트 주입 방식 ("user", "system", "developer")
    allow_parallel: bool = False             # 도구 병렬 실행 허용 여부
    extras: Dict[str, Any] = field(default_factory=dict) # 핸들러 특정 커스텀 메타데이터

@dataclass
class EdgeRelation:
    """에이전트 스텝 간의 전이(Transition) 구조"""
    target: str                              # 다음 노드 ID
    edge_type: EdgeMode = EdgeMode.DIRECT    
    condition: Optional[str] = None

@dataclass
class Fragment:
    """중간 표현체 (IR) - 하나의 에이전트 스텝(Phase)"""
    id: str
    type: NodeType                           
    attributes: AgentAttributes = field(default_factory=AgentAttributes)
    relations: List[EdgeRelation] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)

@dataclass
class FragmentSig:
    """에이전트가 실행할 전체 컨텍스트 생태계 (DAG 구조체)"""
    entry_point: str
    nodes: Dict[str, Fragment] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)