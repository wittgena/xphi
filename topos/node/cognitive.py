# topos.node.cognitive
import asyncio
import dspy
from typing import Any, Dict
from arch.contract.protocol import proto
from topos.proto.flow import ProtoFlow, FlowState
from topos.state.node import ToposNode, NodeType
from phase.bound.plane.emitter import get_emitter

log = get_emitter(__name__)

# ---------------------------------------------------------
# [Phase 1: Pure Cognitive Engine] 
# 위상이나 통신을 전혀 모르는 순수 수학적 인지 블록 (DSPy)
# ---------------------------------------------------------
class TaskRoutingSignature(dspy.Signature):
    """주어진 컨텍스트를 분석하여 다음 위상(Next Node)과 도구를 추론합니다."""
    user_prompt = dspy.InputField()
    current_context = dspy.InputField()
    
    reasoning = dspy.OutputField(desc="라우팅 결정에 대한 논리적 추론")
    tool_plan = dspy.OutputField(desc="필요한 도구 목록")
    next_node_id = dspy.OutputField(desc="위상 그래프 상의 다음 대상 노드 ID")

class CognitiveModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.engine = dspy.ChainOfThought(TaskRoutingSignature)

    def forward(self, user_prompt: str, current_context: str):
        return self.engine(user_prompt=user_prompt, current_context=current_context)


# ---------------------------------------------------------
# [Phase 2: Topological Node (Execution Plane)]
# Topos 시스템의 컨벤션(@proto, FlowState)을 엄격히 준수하는 런타임 노드
# ---------------------------------------------------------
@proto(kind=NodeType.CORE, sequence=["process", "mutate_state"])
class CognitiveNode(ToposNode):
    """
    DSPy 인지 엔진을 탑재한 Topos 실행 노드.
    FlowState를 받아 인지 연산을 수행하고, 상태를 변이(Mutate)시켜 반환합니다.
    """
    def __init__(self, spec: Dict[str, Any], pool: Any = None):
        super().__init__(spec, pool)
        # 인지 엔진을 인스턴스로 탑재 (의존성 주입)
        self.cognitive_engine = CognitiveModule()

    async def process(self, state: FlowState) -> FlowState:
        """ToposRuntime이 호출하는 표준 인터페이스 컨벤션"""
        payload = state.flow.payload
        user_prompt = payload.get("prompt", "")
        context = str(state.state.get("global_context", {}))
        
        log.info(f"[{self.__node_id__}] 🧠 FlowState 수신. DSPy 인지 연산 시작...")

        # 블로킹 방지를 위한 스레드 격리 (컨벤션 준수)
        result = await asyncio.to_thread(
            self.cognitive_engine.forward,
            user_prompt=user_prompt,
            current_context=context
        )

        log.info(f"[{self.__node_id__}] 🎯 판단 완료: {result.reasoning} -> Next: {result.next_node_id}")

        # --- 상태 변이 (State Mutation) 및 방향 지시 ---
        # 이전처럼 self.route()라는 인프라 메서드를 직접 부르는 불법을 저지르지 않습니다.
        # 단지 FlowState의 페이로드를 업데이트하고, 다음 목적지만 기록하여 반환합니다.
        
        state.flow.payload["tool_plan"] = result.tool_plan
        state.flow.payload["cognitive_reasoning"] = result.reasoning
        
        # 그래프 위상에서의 다음 목적지(Edge) 설정
        state.next_target = result.next_node_id 

        return state