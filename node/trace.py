# plane.node.trace
import asyncio
from typing import Tuple, List, Dict, Any
from flow.topos import ToposFlow, FlowState
from model.topos.runtime import (
    Inversion,
    ToposSpec,
    NodeType,
    ToposNode,
    TransRule,
    LinkerNode
)
from plane.log import get_logger

log = get_logger("node.trace")

def print_topos(node: ToposNode, indent=0):
    marker = "[CORE]" if node.kind == NodeType.CORE else "[VIRT]"
    detail = (
        f"-> {node.ref_target}"
        if node.kind == NodeType.SYMLINK
        else f":: {node.content}"
    )
    print("  " * indent + f"{marker} {node.name} {detail}")
    for child in node.children.values():
        print_topos(child, indent + 1)

def emit_init_flow() -> ToposFlow:
    """Φ₀ → Ψ (init emission)"""
    root_topo = ToposNode("root", NodeType.ANCHOR, content="System Root")
    self_topo = ToposNode("self", NodeType.ANCHOR, content="Self Boundary")

    self_topo.children["meta"] = ToposNode(
        "meta", NodeType.SYMLINK, ref_target="meta"
    )
    self_topo.children["ext"] = ToposNode(
        "ext", NodeType.SYMLINK, ref_target="ext"
    )
    root_topo.children["self"] = self_topo
    initial_state = {
        "topology_root": root_topo,
        "residues": []
    }

    print("\n[Φ₀] Initial Topology")
    print_topos(root_topo)
    return ToposFlow(
        payload={"init_state": initial_state},
        aspect="init"
    )

def inject_spec(flow: ToposFlow) -> ToposFlow:
    """Ψ → Ψ (spec injection)"""
    spec = ToposSpec(
        phase_name="S1_INTERNALIZED",
        structure={},
        rules=[
            TransRule(
                source_name="meta",
                target_name="metaflow",
                target_kind=NodeType.CORE,
                action="INVERT"
            ),
            TransRule(
                source_name="ext-phase",
                target_name="projection",
                target_kind=NodeType.CORE,
                action="INVERT"
            )
        ]
    )

    flow.payload["target_spec"] = spec
    flow.aspect = "transition_command"

    return flow

async def resonance(flow: ToposFlow) -> FlowState:
    """Ψ → Φ′ → Φ → Ψ′ (runtime resonance)"""
    ctx = FlowState(flow, flow.payload["init_state"])

    ## minimal runtime (1-node graph)
    node_registry = {
        "LINKER": (LinkerNode, Inversion)
    }

    queue: List[Tuple[str, FlowState]] = [("LINKER", ctx)]

    while queue:
        node_id, current_ctx = queue.pop(0)
        node_cls, op_cls = node_registry[node_id]
        node = node_cls({"next": "END"})
        operator = op_cls()
        print(f"\n[Φ′] Executing: {node_cls.__name__}")
        next_steps = await node.run(
            current_ctx.flow,
            operator,
            current_ctx
        )

        ## Ψ′ propagation
        for next_node_id, new_ctx in next_steps:
            if next_node_id != "END":
                queue.append((next_node_id, new_ctx))
            else:
                return new_ctx

    return current_ctx

async def main():
    print("\n## FLOW START")

    ## Φ₀ → Ψ
    flow = emit_init_flow()

    ## Ψ enrichment
    flow = inject_spec(flow)

    ## Ψ → Φ′ → Φ → Ψ′
    final_ctx = await resonance(flow)

    print("\n[Φ₁] Topology After Inversion")
    print_topos(final_ctx.state["topology_root"])
    print("\n[residue]")
    for r in final_ctx.state["residues"]:
        print(f"  [{r['type'].value}] {r['msg']}")


if __name__ == "__main__":
    asyncio.run(main())