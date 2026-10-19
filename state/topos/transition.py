# xphi.state.topos.transition
import asyncio
import json
import time
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from xphi.arch.contract.event.next import next_id
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.bound.adapter.settlement import ClearingAdapter, TransactionReceipt
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.node.runtime.wasm.quota import Tier
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.runtime.wasm.gateway import GatewayWasm 

epoch_log = get_emitter("fsm.epoch")

class EdgeFlow(Enum):
    ZERO = "ZERO"
    COLLAPSED = "COLLAPSED"
    COHERENT = "COHERENT"
    FRAGMENTED = "FRAGMENTED"
    ANCHOR = "ANCHOR"

    @property
    def symbol(self) -> str:
        mapping = {
            "ZERO": "0",
            "COLLAPSED": "Φ⁻",
            "COHERENT": "Φ⁺",
            "FRAGMENTED": "Φᶠ",
            "ANCHOR": "Ψᴰ"
        }
        return mapping[self.value]


class FlowTransition:
    """WASM 게이트웨이(FSM)와 통신하여 토폴로지 전이 상태 및 비용 정산 Anchor 도달을 추적하는 셸(Shell)"""
    def __init__(self, origin: str = "0"):
        self.id: str = next_id()
        self.origin: str = origin
        self.edge: EdgeFlow = EdgeFlow.ZERO
        self.reflective: bool = True
        self.reversible: bool = True
        self.memory: List[Dict[str, Any]] = []
        self.anchored_target: Optional[str] = None
        self.future: Optional[asyncio.Future] = None
        
        self.gateway = GatewayWasm()
        self._reset_future()

    def _reset_future(self) -> None:
        if self.future and not self.future.done():
            self.future.cancel()
        self.future = asyncio.Future()

    def record(self, message: str, state_change: Optional[EdgeFlow] = None) -> None:
        prev_sym = self.edge.symbol
        log_entry = {"event": message, "previous_state": prev_sym}
        if state_change:
            log_entry["new_state"] = state_change.symbol
            self.edge = state_change
        self.memory.append(log_entry)

    def _apply_event(self, event: dict) -> None:
        fsm_state = {
            "origin": self.origin,
            "edge": self.edge.value,
            "reflective": self.reflective,
            "reversible": self.reversible,
            "anchored_target": self.anchored_target
        }
        
        receipt = self.gateway.execute_flow_transition_fsm(fsm_state, event)
        if not receipt.get("success"):
            epoch_log.error(f"FSM Gateway Execution Failed: {receipt.get('revert_reason')}")
            raise RuntimeError(f"FSM Panic: {receipt.get('revert_reason')}")
            
        next_state = receipt.get("next_fsm_state", {})
        self.edge = EdgeFlow(next_state.get("edge", self.edge.value))
        self.anchored_target = next_state.get("anchored_target")
        if "command" in receipt and receipt["command"]:
            self._handle_command(receipt["command"])

    def _handle_command(self, cmd: dict) -> None:
        ctype = cmd.get("command_type")
        if ctype == "ResolveAnchorCmd":
            if self.future and not self.future.done():
                self.future.set_result(cmd.get("success", True))
            addr = cmd.get("resource_address")
            self.record(f"Anchored state to {addr}")
            
        elif ctype == "FractureCmd":
            if self.future and not self.future.done():
                self.future.set_result(cmd.get("success", False))
                
        elif ctype == "ResetCmd":
            self._reset_future()
            self.record("Reversible exit declared. Returned to 0.")
            
        elif ctype == "HaltFsmCmd":
            reason = cmd.get("reason", "Invalid FSM Transition")
            raise PermissionError(f"FSM Halted: {reason}")

    def bind(self, target_phase: EdgeFlow) -> None:
        self._apply_event({
            "event_type": "Bind", 
            "target_phase": target_phase.value
        })
        self.record(f"Bound to phase {target_phase.symbol}")

    def threshold_test(self, lmbda: float, tau: float) -> bool:
        self._apply_event({
            "event_type": "ThresholdTest", 
            "lmbda": lmbda, 
            "tau": tau
        })
        is_coherent = (self.edge == EdgeFlow.COHERENT)
        self.record(f"Threshold test: λ({lmbda}) vs τ({tau})", state_change=self.edge)
        return is_coherent

    def reach_anchor(self, resource_address: str) -> None:
        self._apply_event({
            "event_type": "ReachAnchor", 
            "resource_address": resource_address
        })

    def fracture_topology(self, lmbda: float, tau: float, force_collapse: bool = False) -> None:
        self._apply_event({
            "event_type": "FractureTopology", 
            "lmbda": lmbda, 
            "tau": tau, 
            "force_collapse": force_collapse
        })
        self.record(f"Topology fractured (force={force_collapse})")

    def unbind_and_reset(self) -> None:
        self._apply_event({"event_type": "UnbindAndReset"})

    async def await_convergence(self, timeout: float = 600.0) -> str:
        try:
            success = await asyncio.wait_for(self.future, timeout=timeout)
            trace = "\n".join([f"[{m.get('new_state', m.get('previous_state', '0'))}] {m['event']}" for m in self.memory])
            if not success:
                trace += "\n[⚠ SYSTEM COLLAPSED] Execution fractured before reaching anchor."
            return trace
        except asyncio.TimeoutError:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            epoch_log.error(f"[{self.origin}] Phase state resolution timed out. Topology Collapsed.")
            return "Execution failed: Timeout reached without convergence."
        except Exception as e:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            epoch_log.error(f"[{self.origin}] Fatal anomaly detected: {e}")
            return f"Execution failed: {e}"


@dataclass
class TopologyResidue:
    edge_state: str
    trajectory_trace: str
    receipt: Optional[TransactionReceipt]


class TransitionManager:
    """FlowTransition, ClearingAdapter, TransactionReceipt 로직을 캡슐화하는 매니저"""
    def __init__(self, origin_name: str, clearing_pub_key: str = "local_clearing_pub_key"):
        self.origin_name = origin_name
        self.transition = FlowTransition(origin=origin_name)
        self.exchange_adapter = ClearingAdapter(clearing_house_pub_key=clearing_pub_key)

    def reset_transition(self) -> None:
        self.transition.unbind_and_reset()

    def record_transition(self, message: str) -> None:
        self.transition.record(message)

    async def settle(self, broker: DphiBroker, cost: float, fuel_consumed: int) -> TopologyResidue:
        entangled_state = {
            "parity": {"topos_id": f"task_{self.transition.id}", "phase_id": 0, "nexus_id": 0},
            "repos": {}
        }
        canonical_payload = EpochSealer.generate_seal_payload(entangled_state, parent_commit_id="genesis")
        
        res = await broker.invoke("seal_epoch", canonical_payload)
        
        signatures = []
        if res.success:
            try:
                signatures = json.loads(res.output).get("signatures", [])
            except json.JSONDecodeError:
                epoch_log.warning(f"[{self.origin_name}] Unparseable response from seal_epoch.")
        else:
            epoch_log.warning(f"[{self.origin_name}] Epoch sealing friction: {res.error}")

        receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        epoch_log.info(f"[{self.origin_name}] 🧾 Transaction Receipt Issued: {receipt.job_id}")
        self.transition.reach_anchor(resource_address=f"urn:surgent:resource:resolved_task_{self.transition.id}")
        residue = TopologyResidue(
            edge_state=self.transition.edge.value,
            trajectory_trace=str(self.transition.edge.symbol),
            receipt=receipt
        )
        self._print_verification_report(residue)
        return residue

    def get_transition(self) -> FlowTransition:
        return self.transition

    def _print_verification_report(self, residue: TopologyResidue) -> None:
        visual_symbol = EdgeFlow(residue.edge_state).symbol if residue.edge_state in [e.value for e in EdgeFlow] else residue.edge_state
        
        epoch_log.info("\n" + "="*60)
        epoch_log.info(f"🌌 [TOPOLOGY FINALIZED] Edge State: {visual_symbol} ({residue.edge_state})")
        if residue.receipt:
            epoch_log.info(f"  Receipt ID (Topos): {residue.receipt.job_id}")
            epoch_log.info(f"  Fuel Burned (Compute): {residue.receipt.fuel_consumed}")
        epoch_log.info("="*60 + "\n")


class EpochSealer:
    @staticmethod
    def generate_seal_payload(entangled_state: Dict[str, Any], parent_commit_id: str = "genesis") -> str:
        signer = NodeSigner.get_instance()
        pubkey = signer.pubkey_hex
        
        parity = entangled_state.get("parity", {})
        repos = entangled_state.get("repos", {})
        timestamp_now = time.time()
        
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity, parent_nexus_id=0, parent_commit_id=parent_commit_id,
            repos=repos, cached_states={}
        )
        
        canonical_bytes = StateAdapter.to_canonical_bytes(anchor_commit)
        signature_hex = signer.sign_payload(canonical_bytes)
        
        epoch_log.debug(f"[Sealer] Generated Ed25519 signature for epoch (Signer: {pubkey[:8]}...)")
        seal_payload_dict = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state=parent_commit_id,
            repos=repos, cached_states={}, timestamp=timestamp_now,
            signers=[pubkey], signatures=[signature_hex], threshold=1, allowed_signers=[pubkey]
        )
        return StateAdapter.to_canonical_bytes(seal_payload_dict).decode('utf-8')