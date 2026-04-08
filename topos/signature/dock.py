# topos.signature.dock
import asyncio
import json
import time
import random
from rocksdict import Rdict, Options
from typing import List, Dict, Any, Optional
from bridge.executor.base import BaseExecutor
from interface.pir import PsiType
from plane.log import get_logger
from anchor.resolver import resolve_path
from model.signature import SignatureBound, TrajectoryXor, ExTrajectory, TraceStep

log = get_logger("signature.dock")
ROCKS_PATH = resolve_path("cache") / "signature.rocks"

class PressureProbe:
    def __init__(self, threshold: float = 0.8):
        self.threshold = threshold
        self.last_pressure = 0.0

    def calculate(self, buffer: List[ExTrajectory], context: Dict[str, Any]) -> bool:
        if not buffer: return False
        scores = [t.score for t in buffer]
        avg_score = sum(scores) / len(scores) if scores else 0
        pressure = (1.0 - avg_score) + context.get("tension", 0.0)
        self.last_pressure = pressure
        return pressure >= self.threshold

class BasisStore:
    def __init__(self, path=ROCKS_PATH):
        opt = Options()
        opt.create_if_missing(True)
        self.db = Rdict(str(path), opt)

    def deposit_basis(self, module_id: str, lineage: List[str], traces: List[dict], context: dict):
        state = {
            "ts": time.time(),
            "lineage": lineage,
            "traces": traces,
            "context": context
        }
        key = f"basis::{module_id}::{state['ts']}".encode()
        self.db[key] = json.dumps(state, ensure_ascii=False)
        self.db[f"basis::{module_id}::latest".encode()] = key
        log.debug(f"[{module_id}] Symbolic Basis deposited to RocksDB.")

    def get_recent_bases(self, module_id: str, limit: int = 5) -> List[dict]:
        # [핵심 수정] keys 리스트 컴프리헨션 추가 (NameError 해결 및 정렬 기준 명확화)
        keys = [
            k for k in self.db.keys() 
            if k.decode().startswith(f"basis::{module_id}::") and not k.decode().endswith("latest")
        ]
        keys.sort(key=lambda k: float(k.decode().split("::")[-1]), reverse=True)
        
        recent_bases = []
        for k in keys[:limit]:
            recent_bases.append(json.loads(self.db[k]))
        return recent_bases

    def close(self):
        self.db.close()

# --- 순수 위상 실행기 ---
class DocksExecutor(BaseExecutor):
    def __init__(self, signature: SignatureBound, synthesizer: TrajectoryXor, store: BasisStore, probe: PressureProbe):
        super().__init__()
        self.phi = signature               
        self.synthesizer = synthesizer     
        self.store = store                 
        self.probe = probe                 
        self.trace_buffer: List[ExTrajectory] = []

    async def execute(self, psi: PsiType) -> List[PsiType]:
        symbol = psi.symbol() if callable(getattr(psi, "symbol", None)) else psi.symbol
        current_lineage = getattr(psi, "_lineage", [self.phi.module_id])

        step = TraceStep(
            module_id=self.phi.module_id,
            inputs={"payload": symbol},
            outputs={"res": "processed"}
        )
        
        score = 0.9 if "valid" in str(symbol) else 0.2
        self.trace_buffer.append(ExTrajectory(lineage=current_lineage, steps=[step], score=score))

        if len(self.trace_buffer) >= 5:
            await self._crystallize()

        psi._lineage = current_lineage + [self.phi.module_id]
        return [psi]

    async def _crystallize(self):
        context = {"tension": 0.5, "enriched_docs": []}
        if self.probe.calculate(self.trace_buffer, context):
            log.warning(f"[{self.phi.module_id}] Gate Opened. Numeric Probe Triggered Shift.")
            active_lineage = self.trace_buffer[0].lineage if self.trace_buffer else []
            trace_dicts = [
                {"steps": [{"inputs": s.inputs} for s in t.steps]} 
                for t in self.trace_buffer
            ]
            
            self.store.deposit_basis(self.phi.module_id, active_lineage, trace_dicts, context)
            recent_bases = self.store.get_recent_bases(self.phi.module_id, limit=3)
            computed_rule = self.synthesizer.synth(recent_bases)
            
            if computed_rule:
                self.phi.mutate(computed_rule)
                log.info(f"[{self.phi.module_id}] Topological Shift Complete.")
                log.info(f"[New State]: {json.dumps(self.phi.dump_state(), indent=2, ensure_ascii=False)}")
                
        self.trace_buffer = []

class MockPsi:
    def __init__(self, content, lineage=None):
        self._content = content
        self._lineage = lineage or ["root"]
    def symbol(self):
        return self._content

async def main():
    store = BasisStore()
    numerical_gate = PressureProbe(threshold=0.6)
    synthesizer = TrajectoryXor(tension_threshold=0.3)
    
    base_sig = SignatureBound(
        module_id="agent_omega",
        base_instructions="Process streams.",
        input_fields=["payload"],
        output_fields=["res"],
        version=1
    )
    
    executor = DocksExecutor(base_sig, synthesizer, store, numerical_gate)
    log.info(f"Autonomous Dock {base_sig.module_id} Online. Structural Symmetry Mode.")

    tick = 1
    while True:
        mock_signal = MockPsi("corrupted error data flow" if random.random() > 0.7 else "valid normal stream")
        processed_signals = await executor.execute(mock_signal)
        if len(executor.trace_buffer) > 0:
            log.debug(f"[Tick {tick}] Ingested Psi. Lineage Propagated: {processed_signals[0]._lineage}")
            
        tick += 1
        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Shutdown signal received. Closing cognitive dock.")