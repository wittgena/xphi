# kernel.state.aggregator
## @lineage: watcher.kernel.state.aggregator
from dataclasses import dataclass, field
from typing import Dict, Any, List

from arch.topos.tunnel.factory import UniversalFacade
from arch.contract.event.psi import PsiCarrier
from phase.wasm.inter.anchor import NodeInterpreter

@dataclass
class CoreState:
    """@state.contract: 결정론적 코어 상태의 구조화된 스냅샷"""
    phase: str
    version: str
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class InternalContext:
    """@worker.payload: 내부 워커가 의사결정을 내리기 위해 참조하는 런타임 컨텍스트"""
    event: PsiCarrier
    state: CoreState
    surface_signals: Dict[str, Any]

class KernelStateAggregator:
    """@context: runtime state + surface signals → InternalContext (Worker Payload)"""
    def __init__(self, interpreter: NodeInterpreter, tunnel: UniversalFacade):
        self.machine = interpreter
        self.tunnel = tunnel

    def snapshot_state(self) -> CoreState:
        """@state.snapshot: 동적 상태를 불변하는 스냅샷으로 동결(Freezing)"""
        return CoreState(
            phase=self.machine.phase,
            version=self.machine.anchor.version,
            meta={} 
        )

    async def retrieve_surface_signals(self, psi: PsiCarrier) -> Dict[str, Any]:
        """@signal.retrieve: 블로킹 없는(Scan) 구조화된 표면 데이터 수집"""
        signals: Dict[str, Any] = {}
        pattern = f"*{psi.tag.split(':')[0]}*"
        try:
            cursor = 0
            collected_keys = []
            while True:
                cursor, keys = await self.tunnel.scan(cursor=cursor, match=pattern, count=100)
                collected_keys.extend(keys)
                if len(collected_keys) >= 5: # Limit boundary
                    break
                    
                if int(cursor) == 0:
                    break
            
            for k in collected_keys[:5]:
                val = await self.tunnel.get(k)
                if val is not None:
                    signals[k] = val
        except Exception as e:
            print(f"[RuntimeStateAggregator] signal retrieval error: {e}")

        return signals

    async def build_context(self, psi: PsiCarrier) -> InternalContext:
        """@context.assemble: 내부 워커용 캡슐 조립"""
        state = self.snapshot_state()
        signals = await self.retrieve_surface_signals(psi)
        return InternalContext(
            event=psi,
            state=state,
            surface_signals=signals
        )