# bridge.ator.runtime
import asyncio
from typing import Dict, Any, List, Tuple
from plane.emitter import get_logger
from flow.ator import AtorFlow, FlowState
from contract.proto.col import get_proto
from interface.pir import (
    AnchorFlow, 
    PhaseInterpreter, 
    PhaseField, 
    PsiCarrier, 
    CarrierType
)
from plane.node.runtime import NodeRuntime
from phi.state.node0 import enter_node0

log = get_logger("ator.runtime")

class AtorRuntime:
    """
    @role: Φ-field mediator
    @flow: Ψ → Φ' → ∂Φ → Node0 → Ψ' → Φ'
    @semantics:
    - PIR = Φ' (judgment operator)
    - boundary = ∂Φ (phase discontinuity)
    - Node0 = transducer (∂Φ → Ψ')
    """
    def __init__(self, entry: str, nodes: Dict[str, Any], runtime_node: NodeRuntime):
        self.entry = entry
        self.nodes = nodes
        self.engine = runtime_node
        
        ## Φ₀: global anchor (phase origin)
        self.global_anchor = AnchorFlow.bootstrap()
        
        ## Φ'-operators per local field
        self.interpreters = self._initialize_interpreters()

        ## τ: minimal boundary threshold
        self.boundary_threshold = 0.5
        log.info(f"[RuntimeAtor] Initialized with Anchor Version: {self.global_anchor.version}")

    def _initialize_interpreters(self) -> Dict[str, PhaseInterpreter]:
        ## field decomposition: {Φ_coherent, Φ_eval, Φ_interference}
        return {
            "ator": PhaseInterpreter(self.global_anchor, field=PhaseField.COHERENT),
            "router": PhaseInterpreter(self.global_anchor, field=PhaseField.EVALUATION),
            "resonance": PhaseInterpreter(self.global_anchor, field=PhaseField.INTERFERENCE),
            "default": PhaseInterpreter(self.global_anchor)
        }

    def _flow_to_carrier(self, flow: AtorFlow, node_type: str) -> PsiCarrier:
        ## Ψ → carrier (ψ → particle embedding in Φ-field)
        field_map = {
            "ator": PhaseField.COHERENT,
            "router": PhaseField.EVALUATION,
            "resonance": PhaseField.INTERFERENCE
        }

        payload = flow.payload.get("status", "psi:ok") if isinstance(flow.payload, dict) else "psi:ok"
        return PsiCarrier(
            kind="status",
            tag="status",
            payload=payload,
            target_field=field_map.get(node_type, PhaseField.COHERENT),
            carrier_type=CarrierType.RECURSIVE,
        )

    def _sync_interpreters(self):
        ## Φ_global shift → propagate to all Φ'
        for interp in self.interpreters.values():
            interp.anchor = self.global_anchor

    def _route_to_boundary(self, ctx):
        ## ∂Φ → Node0 mapping
        return [("NODE0", ctx)]

    async def _process_queue_loop(self):
        step = 0
        log.info("[RuntimeAtor] Attached to RuntimeNode queue.")
        while self.engine.running:
            try:
                node_name, ctx = await self.engine.psi_queue.get()
                step += 1

                ## terminal condition (Φ closure)
                if node_name == "UGA":
                    log.info(f"[Step {step}] Closure Reached. Final State: {ctx.state}")
                    self.engine.psi_queue.task_done()
                    continue

                ## Node0: ∂Φ → Ψ' (boundary transduction)
                if node_name == "NODE0":
                    log.warning(f"[Step {step}] Entering Node0 boundary context")

                    interp = self.interpreters["default"]
                    with enter_node0(interp, "runtime") as n0:
                        ## Ψ collapse → sanitized Ψ'
                        if isinstance(ctx.flow.payload, dict):
                            ctx.flow.payload["status"] = "psi:recovered"

                    ## remove ∂Φ mark → re-enter Φ-field
                    ctx.state.pop("boundary", None)

                    ## re-injection into entry manifold
                    await self.engine.psi_queue.put((self.entry, ctx))
                    self.engine.psi_queue.task_done()
                    continue

                ## normal node (F_op)
                node = self.nodes[node_name]
                node_cls = node.__class__
                p = get_proto(node_cls)
                if not p:
                    raise RuntimeError(f"[{node_name}] Missing @proto metadata.")

                node_type = getattr(p, "kind", "default")

                ## Φ' (PIR judgment)
                interp = self.interpreters.get(node_type, self.interpreters["default"])
                carrier = self._flow_to_carrier(ctx.flow, node_type)
                new_anchor = interp.process(carrier, mediator=self)

                ## ∂Φ generation (inversion)
                if new_anchor.version > self.global_anchor.version:
                    log.warning(f"[Step {step}] Singularity detected")

                    ## Φ → Φ' (global shift)
                    self.global_anchor = new_anchor
                    self._sync_interpreters()

                    ctx.state["boundary"] = "inversion"
                    if isinstance(ctx.flow.payload, dict):
                        ctx.flow.payload["status"] = "delta:resolved"

                ## ∂Φ generation (collapse potential)
                # if carrier.collapse_risk > self.boundary_threshold:
                #     ctx.state["boundary"] = "collapse"

                ## [수정된 부분] 공간이 붕괴되었다면 F_op를 멈추고 즉시 Node0로 우회
                if ctx.state.get("boundary"):
                    log.warning(f"[Step {step}] Topology fractured. Bypassing F_op and routing to NODE0.")
                    next_steps = self._route_to_boundary(ctx)
                else:
                    log.info(f"[Step {step}] Executing F_op on Node: {node_name}")
                    if hasattr(node, "bound_operator") and node.bound_operator is not None:
                        operator = node.bound_operator
                        log.info(f"  [DI] Injecting dynamic operator: {type(operator).__name__}")
                    else:
                        operator_type = p.sequence[1]
                        operator = operator_type()
                        log.info(f"  [DI] Injecting fallback operator: {type(operator).__name__}")

                    next_steps = await node.run(ctx.flow, operator, ctx)
                
                for nxt_node, nxt_ctx in next_steps:
                    await self.engine.psi_queue.put((nxt_node, nxt_ctx))

                self.engine.psi_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error during node execution: {e}", exc_info=True)

    def _control_flow(self, next_steps, ctx):
        controlled = []
        for nxt_node, nxt_ctx in next_steps:
            ## Φ closure
            if nxt_node == "END":
                controlled.append((nxt_node, nxt_ctx))
                continue

            ## local halt (absorbing state)
            if getattr(nxt_ctx, "state", {}).get("halt"):
                continue

            ## pressure overflow (Φ instability)
            if self.engine.psi_queue.qsize() > 1000:
                log.warning("Backpressure triggered, dropping flow")
                continue

            ## ∂Φ interception → Node0
            if nxt_ctx.state.get("boundary"):
                log.warning(f"[Boundary] Routing to Node0: {nxt_node}")
                return self._route_to_boundary(nxt_ctx)

            controlled.append((nxt_node, nxt_ctx))
        return controlled

    def attach(self):
        ## attach controller to runtime manifold
        controller_task = asyncio.create_task(self._process_queue_loop())
        self.engine.loop_tasks.append(controller_task)
