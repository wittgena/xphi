# kernel.topos.state.node0
## @lineage: kernel.phase.state.node0
## @lineage: kernel.state.node0
## @lineage: watcher.kernel.state.node0
from typing import List, Iterator
from contextlib import contextmanager
import uuid
from arch.contract.event.psi import PsiCarrier
from watcher.plane.emitter import get_emitter
from kernel.dphi.wasm.inter.anchor import NodeInterpreter, AnchoredIR, AnchorFlow

log = get_emitter("state.node0")

class Node0State:
    """origin.point for phase cycles: Provides phase reset, recursive reflection, and safe signal reprocessing"""
    def __init__(self, node_id: str):
        self.context_id = f"ctx-0-{uuid.uuid4().hex[:8]}"
        self.node_id = node_id
        ## node0 always holds a pure origin anchor
        self.origin_anchor: AnchoredIR = AnchorFlow.bootstrap()

    def sync_to_origin(self, interpreter: NodeInterpreter) -> NodeInterpreter:
        log.trace(f"[node0:{self.context_id}] Resetting interpreter for {self.node_id}")
        interpreter.anchor = self.origin_anchor
        interpreter.phase = "PHASE_IDLE"
        return interpreter

    def quarantine_collapse_log(self, collapse_log: List[PsiCarrier]) -> List[PsiCarrier]:
        recovered_signals = []
        for psi in collapse_log:
            ## Example condition: If the signal caused a phase tension/error
            if "error" in psi.kind or "panic" in psi.kind:
                log.trace(f"[node0] Quarantining signal: {psi.symbol}")
                recovered_signals.append(
                    PsiCarrier(
                        source=psi.source,
                        kind=f"{psi.kind}:quarantined",
                        tag=psi.tag
                    )
                )
            else:
                ## Normal unhandled signals are prepped for re-evaluation in the new phase
                log.trace(f"[node0] Prepping signal for retry: {psi.symbol}")
                recovered_signals.append(
                    PsiCarrier(
                        source=psi.source,
                        kind=f"{psi.kind}:retry",
                        tag=psi.tag
                    )
                )
        return recovered_signals

@contextmanager
def enter_node0(interpreter: NodeInterpreter, node_id: str) -> Iterator[Node0State]:
    log.trace(f"[node0] {node_id} ENTERS origin context")
    n0_state = Node0State(node_id)
    try:
        n0_state.sync_to_origin(interpreter)
        yield n0_state
    except Exception as e:
        log.trace(f"[node0] Exception caught inside origin context: {e}. Forcing hard reset.")
        n0_state.sync_to_origin(interpreter)
    finally:
        log.trace(f"## [node0] {node_id} EXITS origin context. Ready at {interpreter.phase}")