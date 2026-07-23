# ops.builder.graph
## @lineage: gov.engine.bounder
import json
import argparse
import networkx as nx
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Tuple

from arch.gov.state.vocab import SigType, SpecKey
from arch.gov.state.schema import FragmentSig
from arch.contract.schema.graph import EntryNode
from arch.contract.schema.resonance import ResonanceGraph, ResonanceNode, NodeRelation
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_logger

log = get_logger("graph.builder")

@dataclass
class DagTestReport:
    """Execution test report returned to the agent pipeline."""
    is_valid: bool
    alignment_errors: List[str] = field(default_factory=list)
    simulation_errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    # Field name preserved for external compatibility; represents simulation compute cost
    metabolic_cost: float = 0.0 
    
    def to_dict(self) -> dict:
        return asdict(self)

class SubgraphExtractor:
    @staticmethod
    def select(graph_data: dict, entry_ctx: EntryNode) -> ResonanceGraph:
        """Extracts a bounded topological subgraph starting from the entry node using BFS."""
        full_nodes = {}
        for k, v in graph_data.get("nodes", {}).items():
            raw_rels = v.get("relations", [])
            v["relations"] = [NodeRelation(**r) if isinstance(r, dict) else r for r in raw_rels]
            full_nodes[k] = ResonanceNode(**v)
        
        entry_id = entry_ctx.entry
        if entry_id not in full_nodes:
            return ResonanceGraph(invariants=[], nodes={})

        selected_ids = {entry_id}
        current_level = {entry_id}
        
        for _ in range(entry_ctx.depth):
            next_level = set()
            for n_id in current_level:
                for rel in full_nodes[n_id].relations:
                    if rel.rel in entry_ctx.valid_relations and rel.target in full_nodes:
                        next_level.add(rel.target)
            selected_ids.update(next_level)
            current_level = next_level

        sub_nodes = {n_id: full_nodes[n_id] for n_id in selected_ids}
        return ResonanceGraph(invariants=[], nodes=sub_nodes)

class TopologyAlignmentTester:
    @staticmethod
    def verify(ir_sig: FragmentSig, codebase_graph: ResonanceGraph) -> List[str]:
        """Verifies if the generated DAG targets valid nodes within the bounded codebase graph."""
        errors = []
        valid_targets = set(codebase_graph.nodes.keys())
        
        for frag_id, frag in ir_sig.nodes.items():
            target_file = frag.attributes.extras.get("file_path")
            if target_file and target_file not in valid_targets:
                errors.append(
                    f"Hallucination Detected: Node '{frag_id}' targets '{target_file}', "
                    f"but this file is outside the bounded context."
                )
        return errors

class DryRunSimulator:
    @staticmethod
    def simulate(runtime_specs: Dict[str, Dict[str, Any]], entry_point: str, max_ticks: int = 100) -> Tuple[List[str], int]:
        """Simulates DAG execution to detect logical flaws. Returns (errors, consumed_ticks)."""
        errors = []
        visited_counts: Dict[str, int] = {}
        current_node = entry_point
        ticks = 0
        
        while current_node != SigType.END.value:
            if ticks > max_ticks:
                errors.append(f"Timeout Exceeded: Potential infinite loop detected. Ticks exceeded {max_ticks}.")
                break
                
            if current_node not in runtime_specs:
                errors.append(f"Broken Link: Node '{current_node}' does not exist in specs.")
                break
                
            spec = runtime_specs[current_node]
            visited_counts[current_node] = visited_counts.get(current_node, 0) + 1
            
            max_fails = spec.get(SpecKey.MAX_FAILURES, 3)
            if visited_counts[current_node] > (max_fails * 2): 
                errors.append(f"Cycle Detected: Node '{current_node}' trapped the execution in an infinite cycle.")
                break
                
            if spec.get(SpecKey.TYPE) == SigType.ROUTER.value:
                current_node = spec.get(SpecKey.DEFAULT_NEXT, SigType.END.value)
            else:
                current_node = spec.get(SpecKey.NEXT, SigType.END.value)
                
            ticks += 1
            
        return errors, ticks

# ============================================================================
# DAG Blueprint Generator & Exporter
# ============================================================================

class DagBlueprintGenerator:
    """Encapsulates NetworkX DAG construction and specification export logic."""
    
    @staticmethod
    def build_executable_dag() -> nx.DiGraph:
        """Constructs an advanced DAG incorporating security, reflection, and parallel execution phases."""
        G = nx.DiGraph()
        
        # Initial context analysis
        G.add_node("node_analyze_intent", 
            type="signature.projector", color="lightblue",
            attributes={"instructions": "Analyze user request and dynamically injected context.", "pressure": 0.2}
        )
        
        # Security routing
        G.add_node("node_security_router",
            type="signature.router", color="orange",
            rules=[
                {"if": {"aspect": "risk_high"}, "next": "node_await_confirmation"},
                {"if": {"aspect": "risk_low"}, "next": "node_execute_parallel_tools"}
            ],
            default_next="node_execute_parallel_tools"
        )
        
        G.add_node("node_await_confirmation",
            type="signature.pending", color="lightcoral",
            attributes={"instructions": "Pause execution. High-risk tools require user confirmation."}
        )
        
        # Execution phase
        G.add_node("node_execute_parallel_tools",
            type="signature.opersig", color="lightgreen",
            attributes={"instructions": "Execute necessary tools in parallel.", "allow_parallel": True, "pressure": 0.6}
        )
        
        # Meta-reflection phase
        G.add_node("node_reflector_evaluation",
            type="signature.reflect", color="gold",
            attributes={"instructions": "Critique tool execution results. Determine iteration necessity.", "pressure": 0.4}
        )
        
        # Evaluation routing
        G.add_node("node_evaluation_router",
            type="signature.router", color="orange",
            rules=[
                {"if": {"aspect": "needs_refinement"}, "next": "node_analyze_intent"}, 
                {"if": {"aspect": "task_complete"}, "next": "node_finalize_response"}
            ],
            default_next="node_finalize_response"
        )
        
        # Finalization
        G.add_node("node_finalize_response",
            type="signature.act", color="plum",
            attributes={"instructions": "Format final text response based on successful execution."}
        )
        
        G.add_node("END", type="sink", color="gray")
        
        # Edge definition
        G.add_edges_from([
            ("node_analyze_intent", "node_security_router"),
            ("node_security_router", "node_await_confirmation"),
            ("node_security_router", "node_execute_parallel_tools"),
            ("node_await_confirmation", "node_execute_parallel_tools"),
            ("node_execute_parallel_tools", "node_reflector_evaluation"),
            ("node_reflector_evaluation", "node_evaluation_router"),
            ("node_evaluation_router", "node_analyze_intent"),
            ("node_evaluation_router", "node_finalize_response"),
            ("node_finalize_response", "END")
        ])
        
        return G

    @staticmethod
    def export_to_runtime_specs(G: nx.DiGraph, entry_point: str) -> dict:
        """Converts the NetworkX DAG into JSON-compatible runtime specifications."""
        specs = {}
        for node_id in G.nodes():
            if node_id == "END":
                continue
                
            node_data = G.nodes[node_id]
            spec = {"type": node_data.get("type")}
            
            if "attributes" in node_data:
                spec["attributes"] = node_data["attributes"]
                
            if node_data.get("type") == "signature.router":
                spec["rules"] = node_data.get("rules", [])
                spec["default_next"] = node_data.get("default_next", "END")
            else:
                successors = list(G.successors(node_id))
                spec["next"] = successors[0] if successors else "END"
            specs[node_id] = spec
            
        return {"entry_point": entry_point, "nodes": specs}

    @staticmethod
    def export_to_mermaid(G: nx.DiGraph) -> str:
        """Generates a Mermaid.js markdown string for DAG visualization."""
        lines = ["graph TD"]
        color_map = {
            "lightblue": "fill:#add8e6,stroke:#333,stroke-width:2px",
            "orange": "fill:#ffa500,stroke:#333,stroke-width:2px",
            "lightcoral": "fill:#f08080,stroke:#333,stroke-width:2px",
            "lightgreen": "fill:#90ee90,stroke:#333,stroke-width:2px",
            "gold": "fill:#ffd700,stroke:#333,stroke-width:2px",
            "plum": "fill:#dda0dd,stroke:#333,stroke-width:2px"
        }
        
        for node_id in G.nodes():
            node_data = G.nodes[node_id]
            
            if node_id == "END":
                lines.append(f"    {node_id}([{node_id}])")
                lines.append(f"    style {node_id} fill:#d3d3d3,stroke:#333,stroke-width:2px")
                continue
                
            style = color_map.get(node_data.get("color"), "fill:#fff,stroke:#333,stroke-width:2px")
            lines.append(f"    {node_id}[{node_id}]")
            lines.append(f"    style {node_id} {style}")

        lines.append("")
        for u, v in G.edges():
            lines.append(f"    {u} --> {v}")
            
        return "\n".join(lines)