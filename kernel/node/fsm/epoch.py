# xphi.kernel.node.fsm.epoch
## @lineage: theoria.phase.model.epoch
import asyncio
import json
import time
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from xphi.arch.bound.event.next import next_id
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.bound.adapter.settlement import ClearingAdapter, TransactionReceipt
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.arch.contract.flow import PhaseFlow, FlowState
from xphi.kernel.node.gan import Message, GanNode
from xphi.kernel.wasm.broker import DphiBroker
from xphi.kernel.wasm.cgroup import Tier
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.gateway import GatewayWasm 

epoch_log = get_emitter("fsm.epoch")

class EdgeFlow(Enum):
    ZERO = "0"
    COLLAPSED = "Φ⁻"
    COHERENT = "Φ⁺"
    FRAGMENTED = "Φᶠ"
    DOMINIUM = "Ψᴰ"

class FlowTransition:
    """WASM 게이트웨이(FSM)와 통신하여 토폴로지 전이 상태 및 비용 정산 Dominium 도달을 추적하는 셸(Shell)"""
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
        log_entry = {"event": message, "previous_state": self.edge.value}
        if state_change:
            log_entry["new_state"] = state_change.value
            self.edge = state_change
        self.memory.append(log_entry)

    def _apply_event(self, event: dict) -> None:
        """이벤트를 WASM FSM으로 전송하고 반환된 상태와 커맨드를 처리합니다."""
        # 1. 현재 상태 DTO화
        fsm_state = {
            "origin": self.origin,
            "edge": self.edge.value,
            "reflective": self.reflective,
            "reversible": self.reversible,
            "anchored_target": self.anchored_target
        }
        
        # 2. WASM 실행
        receipt = self.gateway.execute_flow_transition_fsm(fsm_state, event)
        if not receipt.get("success"):
            epoch_log.error(f"FSM Gateway Execution Failed: {receipt.get('revert_reason')}")
            raise RuntimeError(f"FSM Panic: {receipt.get('revert_reason')}")
            
        # 3. WASM이 결정한 상태(Next State) 동기화
        next_state = receipt.get("next_fsm_state", {})
        self.edge = EdgeFlow(next_state.get("edge", self.edge.value))
        self.anchored_target = next_state.get("anchored_target")
        
        # 4. WASM이 지시한 사이드 이펙트(Command) 수행
        if "command" in receipt and receipt["command"]:
            self._handle_command(receipt["command"])

    def _handle_command(self, cmd: dict) -> None:
        ctype = cmd.get("command_type")
        
        if ctype == "ResolveDominiumCmd":
            if self.future and not self.future.done():
                self.future.set_result(cmd.get("success", True))
            addr = cmd.get("resource_address")
            self.record(f"Anchored Dominium to {addr}")
            
        elif ctype == "FractureCmd":
            if self.future and not self.future.done():
                self.future.set_result(cmd.get("success", False))
                
        elif ctype == "ResetCmd":
            self._reset_future()
            self.record("Reversible exit declared. Returned to 0.")
            
        elif ctype == "HaltFsmCmd":
            reason = cmd.get("reason", "Invalid FSM Transition")
            raise PermissionError(f"FSM Halted: {reason}")

    # ----------------------------------------------------
    # 💡 Public APIs (Trigger Events)
    # ----------------------------------------------------
    def bind(self, target_phase: EdgeFlow) -> None:
        self._apply_event({
            "event_type": "Bind", 
            "target_phase": target_phase.value
        })
        self.record(f"Bound to phase {target_phase.value}")

    def threshold_test(self, lmbda: float, tau: float) -> bool:
        self._apply_event({
            "event_type": "ThresholdTest", 
            "lmbda": lmbda, 
            "tau": tau
        })
        is_coherent = (self.edge == EdgeFlow.COHERENT)
        self.record(f"Threshold test: λ({lmbda}) vs τ({tau})", state_change=self.edge)
        return is_coherent

    def reach_dominium(self, resource_address: str) -> None:
        self._apply_event({
            "event_type": "ReachDominium", 
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
                trace += "\n[⚠️ SYSTEM COLLAPSED] Execution fractured before reaching dominium."
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
        """Dominium 비용 정산을 수행하고 TopologyResidue를 반환합니다."""
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
        self.transition.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{self.transition.id}")
        
        residue = TopologyResidue(
            edge_state=self.transition.edge.value,
            trajectory_trace=str(self.transition.edge),
            receipt=receipt
        )
        self._print_verification_report(residue)
        return residue

    def get_transition(self) -> FlowTransition:
        return self.transition

    def _print_verification_report(self, residue: TopologyResidue) -> None:
        epoch_log.info("\n" + "="*60)
        epoch_log.info(f"🌌 [TOPOLOGY FINALIZED] Dominium Edge State: {residue.edge_state}")
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


# ==========================================
# 4. Routing & Boundary Management
# ==========================================
class _LocalBoundary:
    """기존의 파편화된 Bound 및 LocalBound 클래스를 하나로 통합한 내부 라우터"""
    def __init__(self, local_registry: Dict[str, Any], broker: Any):
        self.registry = local_registry
        self.broker = broker

    async def _execute_atomic_transition(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        payload = {
            "intent_action": f"trans_{flow.id[:8]}",
            "intent_payload": {"target": target_id, "manifold_keys": list(flow.payload.keys())},
            "evolution_ctx": {"phase_root": ctx.state.get("phase_root", {}), "external_rules": ctx.state.get("external_rules", [])}
        }
        try:
            res = json.loads(await self.broker.execute("execute_transition", payload))
            if not res.get("is_authorized"):
                return False
            if res.get("final_root"): 
                ctx.state["phase_root"] = res["final_root"]
            if res.get("all_residues"): 
                ctx.state.setdefault("residues", []).extend(res["all_residues"])
            return True
        except Exception:
            return False

    async def emit(self, target_id: str, flow: PhaseFlow, ctx: FlowState) -> bool:
        if not await self._execute_atomic_transition(target_id, flow, ctx): 
            return False
        
        target_node = self.registry.get(target_id)
        if not target_node: 
            return False

        if isinstance(target_node, GanNode):
            target_node.post_message(Message("flow_ingress", flow_id=flow.id, payload={"flow": flow, "ctx": ctx}))
        elif hasattr(target_node, "run"):
            asyncio.create_task(self._legacy_run(target_node, flow, ctx))
        return True

    async def _legacy_run(self, target_node: Any, flow: PhaseFlow, ctx: FlowState):
        for next_node_id, next_ctx in await target_node.run(flow, ctx.state.get("operator"), ctx):
            if next_node_id != "END": 
                await self.emit(next_node_id, flow, next_ctx)


class EpochFolder:
    def __init__(self, broker: Any, redis_pool: Optional[Any] = None):
        self.local_registry = {}
        self.broker = broker
        self.local_bound = _LocalBoundary(self.local_registry, broker)
        
        if redis_pool:
            epoch_log.warning("[EpochFolder] Remote redis_pool routing is deprecated. Forcing LocalBoundary.")

    def fold(self, active_nodes: Dict[str, Any], topology_spec: Dict[str, Any]) -> Dict[str, Any]:
        self.local_registry.update(active_nodes)
        
        for node_id, spec in topology_spec.items():
            if source_node := self.local_registry.get(node_id):
                if not hasattr(source_node, "boundaries"): 
                    source_node.boundaries = {}
                
                for target_id in spec.get("edges", []):
                    source_node.boundaries[target_id] = self.local_bound
                    
        return self.local_registry