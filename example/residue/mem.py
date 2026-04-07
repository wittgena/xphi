# example.residue.mem
"""
@flow: Ψ → Xe → ∂Φ(boundary) → τ(accumulation) → rupture → inversion → residue → Φ'
@phase: residue is crystallized trace of rupture (Φ′ seed)
"""
import asyncio
import uuid
import inspect
from typing import List
from bridge.interface.pir import PsiType
from bridge.executor.base import BaseExecutor
from node.runtime import NodeRuntime
from memory.signature.dock import MemProbe, MemAdapter, PhaseTensionCore, Bound, ResidueStore
from anchor.resolver import resolve_path
from active.log import get_logger

log = get_logger("residue.mem")

class PhaseField(type(BaseExecutor)): 
    """@phase.bound: assigns unique ∂Φ to each Xe instance"""
    def __new__(mcs, name, bases, namespace):
        namespace['bound_id'] = f"bound.{uuid.uuid4().hex[:4]}"
        # BaseExecutor의 메타클래스가 가진 __new__를 호출하게 됨
        return super().__new__(mcs, name, bases, namespace)

class XeCont(BaseExecutor, metaclass=PhaseField):
    """
    @entity: autonomous phase carrier (Ψ processor without interpretation)
    @flow: Ψ → accumulate → τ → rupture → inversion → re-bind
    """
    def __init__(self, bound: Bound, store: ResidueStore, ex: str = "void", origin: str = "void"):
        super().__init__()
        self.trace_id = f"base.{uuid.uuid4().hex[:6]}"
        self.ex = ex
        self.origin = origin
        self.bound = bound
        self.store = store

    async def execute(self, psi: PsiType) -> List[PsiType]:
        """
        @loop: Ψ input -> ∂Φ.absorb -> τ evaluation -> {rupture | saturation} -> Ψ resonance (unchanged)
        @exist: closed.loop driven by external signal flow
        """
        ## step.1: Ψ → ∂Φ (absorption)
        batch_payload = [{"payload": psi.symbol()}]
        self.bound.absorb(batch_payload)
        
        ## step.2: τ evaluation (phase tension)
        decision = self.bound.evaluate()
        if decision == "DEPOSIT":
            """
            @phase.transition:
            τ ≥ threshold → rupture

            rupture:
              ∂Φ → snapshot → residue → inversion
            """
            snap = self.bound.snapshot()
            
            ## Φ′ crystallization (residue persistence)
            self.store.deposit(snap)
            self.bound.commit()

            ## inversion (Φ → Φ⁻ → Φ′ rebind)
            ext_base = self._ext__()
            self.log.signal(f"[Rupture] 구조 파열 및 위상 반전: {self.trace_id} -> {ext_base.trace_id}")
            
            ## overwrite local phase state (re-binding)
            self.ex = ext_base.ex
            self.origin = ext_base.trace_id
        else:
            """@phase.state: τ < threshold → saturation"""
            self.log.info(f"[Saturation] self-reference (pressure:{self.bound.pressure:.3f}): {self.trace_id}")

        ## step.3: Ψ resonance (identity preserved)
        return [psi]

    def _ext__(self) -> 'XeCont':
        """@flow: ex → overflow → inversion → Base.bind()"""
        overflowed_ex = f"overflow.{self.ex}"
        inverted_state = f"inversion.{overflowed_ex}"
        base_state = f"Base.bind({inverted_state})"
        
        # Inversion된 새로운 상태를 반환
        return XeCont(
            bound=self.bound, 
            store=self.store, 
            ex=base_state, 
            origin=self.trace_id
        )

    def __repr__(self):
        return f"<XeCont {self.bound_id} id:{self.trace_id}, mem='{self.ex}'>"

def main():
    """
    @phase.activation:
    field(∂Φ) → Xe → RuntimeNode → Ψ loop

    @composition:
    MemProbe → MemAdapter → PhaseTensionCore → Bound
                                ↓
                             XeCont
                                ↓
                           RuntimeNode
    """
    CACHE_ROOT = resolve_path("cache")
    XE_MEM = CACHE_ROOT / "residue.mem.json"
    ROCKS_PATH = CACHE_ROOT / "residue_rocks.db"

    ## phase.1: ∂Φ field construction (tension measurement)
    probe = MemProbe(XE_MEM)
    mem_adapter = MemAdapter(probe)
    core = PhaseTensionCore(mem_adapter)
    bound = Bound(core)
    store = ResidueStore(path=ROCKS_PATH)

    ## phase.2: Xe binding (phase carrier instantiation)
    xe_ex = XeCont(bound=bound, store=store, ex="void")
    
    ## phase.3 runtime activation (Ψ loop carrier)
    node = NodeRuntime(executor=xe_ex)
    
    log.info(f"[*] Xe phase runtime start: {xe_ex}")
    asyncio.run(node.start())

if __name__ == "__main__":
    main()