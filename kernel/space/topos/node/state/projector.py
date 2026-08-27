# xphi.kernel.space.topos.node.state.projector
## @lineage: kernel.space.topos.node.state.projector
## @lineage: arch.topos.node.state.projector
## @lineage: arch.gov.state.projector
## @lineage: ops.tester.state.projector
from typing import Dict, Any, Optional
from xphi.kernel.space.topos.node.state.vocab import NodeType, SigType, EdgeMode, SpecKey, DEFAULT_TYPE_MAP
from xphi.kernel.space.topos.node.state.schema import Fragment, FragmentSig, AgentAttributes

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("state.projector")

class StateProjector:
    """
    @phase: Φ′(IR) → Runtime Spec
    @desc: 중간 표현체(FragmentSig)를 GraphOrchestrator(DagOrganizer)가 실행 가능한 저수준(Low-level) 딕셔너리 스펙으로 Lowering
    """
    def __init__(self, type_map: Optional[Dict[NodeType, SigType]] = None):
        """외부에서 주입 가능한 타입 맵 (기본값: DEFAULT_TYPE_MAP)"""
        self.type_map = type_map or DEFAULT_TYPE_MAP

    def validate_fragment(self, frag: Fragment) -> bool:
        """노드 타입의 유효성을 검증하는 훅"""
        if frag.type not in self.type_map:
            log.warning(f"[Projector] Unknown type: '{frag.type}'. Falling back to OPERSIG.")
            return False
        return True

    def project(self, signature: FragmentSig) -> Dict[str, Dict[str, Any]]:
        specs = {}

        for frag_id, frag in signature.nodes.items():
            self.validate_fragment(frag)
            
            ## 1. 노드 기본 규격 구성
            signature_type = self.type_map.get(frag.type, SigType.OPERSIG)
            spec: Dict[str, Any] = {SpecKey.TYPE: signature_type}
            
            ## 2. AgentAttributes 매핑 (DagOrganizer가 기대하는 위치로 언패킹)
            attrs: AgentAttributes = frag.attributes
            
            # DagOrganizer._apply_node_pressure가 읽는 컨텍스트
            spec[SpecKey.ATTRIBUTES] = {
                "instructions": attrs.instructions,
                "pressure": attrs.pressure,
                "allow_parallel": attrs.allow_parallel,
                **attrs.extras
            }
            # DagOrganizer._is_node_fatigued가 읽는 위상 붕괴 방어선 (루트 레벨)
            spec[SpecKey.MAX_FAILURES] = attrs.max_failures

            # 3. 엣지(Relations)를 기능별로 분류
            conditional_edges = [e for e in frag.relations if e.edge_type == EdgeMode.CONDITIONAL]
            unconditional_edges = [e for e in frag.relations if e.edge_type == EdgeMode.DIRECT]
            fallback_edges = [e for e in frag.relations if e.edge_type == EdgeMode.FALLBACK]

            # 4. Fallback (예외 발생 시 우회 경로) 설정
            if fallback_edges:
                # DagOrganizer except Exception 블록에서 사용하는 방어 노드
                spec[SpecKey.FALLBACK] = fallback_edges[0].target

            # 5. 분기 라우팅 처리 (Projector의 핵심 임무)
            if conditional_edges:
                # Organizer가 조건 분기를 처리할 수 있도록 Router 노드를 암시적으로 생성하여 삽입
                switch_id = f"{frag_id}_switch"
                spec[SpecKey.NEXT] = switch_id  # 현재 노드의 다음을 라우터로 연결
                
                rules = []
                for edge in conditional_edges:
                    rules.append({
                        SpecKey.IF_COND: {SpecKey.ASPECT: edge.condition}, 
                        SpecKey.NEXT: edge.target
                    })
                
                specs[switch_id] = {
                    SpecKey.TYPE: SigType.ROUTER,
                    SpecKey.RULES: rules,
                    SpecKey.DEFAULT_NEXT: unconditional_edges[0].target if unconditional_edges else SigType.END
                }

            # 6. 단일 방향 직진 흐름
            elif unconditional_edges:
                # 단일 타겟이면 문자열, 병렬 실행 등 다중 타겟일 경우 리스트 허용
                targets = [e.target for e in unconditional_edges]
                spec[SpecKey.NEXT] = targets[0] if len(targets) == 1 else targets
            else:
                spec[SpecKey.NEXT] = SigType.END
                
            specs[frag_id] = spec
            
        return specs