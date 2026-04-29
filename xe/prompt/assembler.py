# xe.prompt.assembler
from dataclasses import dataclass, field
from typing import Dict, Any, List
import redis.asyncio as redis_async
from bridge.psi import PsiCarrier
from node.interpreter import NodeInterpreter

@dataclass
class CoreState:
    """@state.contract: 결정론적 코어 상태의 구조화된 스냅샷"""
    phase: str
    version: str
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class InternalContext:
    """@worker.payload: 내부 워커(Dispatcher, Phase Handler 등)가 의사결정을 내리기 위해 참조하는 완전한 컨텍스트"""
    event: PsiCarrier
    state: CoreState
    evidence: Dict[str, Any]

class ExtAssembler:
    """
    @context: runtime state + surface evidence → InternalContext (Payload)
    """
    def __init__(self, interpreter: NodeInterpreter, redis: redis_async.Redis):
        self.machine = interpreter
        self.redis = redis

    def snapshot_state(self) -> CoreState:
        """@state.snapshot: 워커가 직접 속성(attribute)으로 접근할 수 있는 객체 반환"""
        return CoreState(
            phase=self.machine.phase,
            version=self.machine.anchor.version,
            meta={} # 향후 PhaseMachine 내부의 메모리 맵 등을 얕은 복사로 전달 가능
        )

    async def retrieve_evidence(self, psi: PsiCarrier) -> Dict[str, Any]:
        """@evidence.retrieve: 단순 문자열 병합이 아닌, Key-Value 형태의 구조화된 데이터 유지"""
        evidence: Dict[str, Any] = {}

        try:
            ## 선택적: Redis 등 Surface에서 연관 데이터를 구조적으로 가져옴
            keys = await self.redis.keys(f"*{psi.tag.split(':')[0]}*")
            for k in keys[:5]:
                val = await self.redis.get(k)
                if val:
                    evidence[k.decode()] = val.decode()
            pass
        except Exception as e:
            print(f"[ContextWorkflow] evidence retrieval error: {e}")

        return evidence

    async def build_context(self, psi: PsiCarrier) -> InternalContext:
        """@context.assemble: 내부 워커가 즉각적으로 연산에 투입할 수 있는 InternalContext 캡슐 조립"""
        state = self.snapshot_state()
        evidence = await self.retrieve_evidence(psi)
        return InternalContext(
            event=psi,
            state=state,
            evidence=evidence
        )