# xphi.arch.topos.runtime
import asyncio
import enum 
import logging
from typing import List, Tuple, Any, Optional, Dict
from dataclasses import field
from flow.surface.emitter import get_emitter
from contract.proto.col import proto, get_proto, Proto
from contract.proto.flow import ProtoFlow, FlowState
from arch.topos.trans import PhaseSpec, TransRule, NodeType
from arch.topos.node import LinkerNode, InversionNode, PhaseNode, ResidueType

log = get_emitter("topos.runtime")

class ToposRuntime:
    def __init__(self, entry: str, nodes: Dict[str, Any], runtime_node: Any):
        self.entry = entry
        self.nodes = nodes
        self.engine = runtime_node

    async def _process_queue_loop(self):
        step = 0
        log.info("[ToposRuntime] Attached. Waiting for signals...")
        while self.engine.running:
            try:
                node_name, ctx = await self.engine.psi_queue.get()
                step += 1

                if node_name == "END":
                    log.info(f"[Step {step}] Terminal reached. Flow Ended.")
                    log.info(f"Final Residues: {ctx.state.get('residues')}")
                    self.engine.psi_queue.task_done()
                    continue

                log.info(f"[Step {step}] Executing Node: {node_name}")
                node = self.nodes[node_name]
                node_cls = node.__class__

                p = get_proto(node_cls)
                if not p:
                    raise RuntimeError(f"[{node_name}] Missing @proto metadata.")

                if ctx.state.get("__reentry__"):
                    ctx.state.pop("__reentry__", None)
                    self.engine.psi_queue.task_done()
                    await self.engine.psi_queue.put((self.entry, ctx))
                    continue

                operator_type = p.sequence[1]
                operator = operator_type() 
                next_steps = await node.run(ctx.flow, operator, ctx)
                for nxt_node, nxt_ctx in next_steps:
                    await self.engine.psi_queue.put((nxt_node, nxt_ctx))

                self.engine.psi_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error: {e}", exc_info=True)

    def attach(self):
        controller_task = asyncio.create_task(self._process_queue_loop())
        self.engine.loop_tasks.append(controller_task)