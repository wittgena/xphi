# flow.analyzer
import ast
import json
import argparse
import sys
import networkx as nx
from pathlib import Path
from collections import Counter
from typing import TypedDict, List, Dict, Any
from dataclasses import dataclass, asdict
from topos.lang.model import LangModel, NodeData, EdgeData, LoopEdgeData, PhaseGraphSchema

class LogicTransformer:
    """
    @phi: Logic Orbit (Pure AST Transformation)
    @flow: source(code) → parse(AST) → extract(import relations) → detect(runtime signals) → Ψ_struct fragments
    """
    RUNTIME_KEYWORDS = {"while", "asyncio", "Queue", "yield", "run_forever"}

    @staticmethod
    def parse_module(path: Path) -> Dict[str, Any]:
        try:
            content = path.read_text(encoding="utf-8")
            tree = ast.parse(content)
            
            ## ∂Φ_runtime: detect potential local topos (loop / async / feedback signals)
            is_topos = any(kw in content for kw in LogicTransformer.RUNTIME_KEYWORDS)
            
            ## Ψ_import: extract dependency edges from AST
            imports = []
            for node in ast.walk(tree):
                lineno = getattr(node, 'lineno', None)
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append({"target": alias.name, "lineno": lineno})
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0: # 상대경로 보정
                        prefix = "." * node.level
                        target = f"{prefix}{node.module}" if node.module else prefix
                        imports.append({"target": target, "lineno": lineno})
                    elif node.module:
                        imports.append({"target": node.module, "lineno": lineno})
                        
            return {"imports": imports, "is_topos": is_topos}
        except Exception:
            return {"imports": [], "is_topos": False}

class ToposAnalyzer:
    """
    @Σ: Topos Orbit (Graph Inference & Resonance)
    @flow: Ψ_fragments → bind(edges) → construct(Φ_graph) → measure(curvature / centrality) → classify(phere) → Φ_dissolved
    """
    def __init__(self):
        self.g = nx.DiGraph()
        self.g_loop = nx.DiGraph()
        self.runtime_hints = {}

    def build_structure(self, module_index: Dict[str, Path], root: Path):
        for mod, path in module_index.items():
            ## Φ_loop: containment structure (filesystem topos)
            rel = path.relative_to(root)
            parent = rel.parent.as_posix() if rel.parent.parts else root.name
            self.g_loop.add_edge(parent, rel.as_posix(), type="contain")

            ## Φ_topos: logical dependency graph
            self.g.add_node(mod)
            parsed = LogicTransformer.parse_module(path)
            self.runtime_hints[mod] = parsed["is_topos"]
            
            ## bind Ψ_import into Φ graph
            for imp in parsed["imports"]:
                target = self._resolve(imp["target"], module_index)
                if target and mod != target:
                    self._add_edge(mod, target, imp["lineno"])

    def _resolve(self, target: str, index: Dict[str, Path]) -> str:
        ## Φ_resolution: map symbolic import → internal module
        if target in index: return target
        for mod in index:
            if mod.startswith(target + ".") or target.startswith(mod + "."):
                return mod
        return None

    def _add_edge(self, src: str, tgt: str, line: int):
        if self.g.has_edge(src, tgt):
            if line and line not in self.g[src][tgt]["linenos"]:
                self.g[src][tgt]["linenos"].append(line)
        else:
            self.g.add_edge(src, tgt, keyword="import", linenos=[line] if line else [])

    def get_dissolve_schema(self, module_index: Dict[str, Path], root: Path) -> PhaseGraphSchema:
        ## curvature metrics (∂Φ_measure)
        degree = dict(self.g.degree)
        in_degree = dict(self.g.in_degree)
        out_degree = dict(self.g.out_degree)
        betweenness = nx.betweenness_centrality(self.g) if self.g.nodes else {}
        
        try: cycles = list(nx.simple_cycles(self.g))
        except: cycles = []

        nodes = []
        phere_counter = Counter()
        absorbable_count = 0

        ## Φ_classification: assign structural roles
        for n in self.g.nodes:
            deg = degree.get(n, 0)
            in_d = in_degree.get(n, 0)
            out_d = out_degree.get(n, 0)
            bc = betweenness.get(n, 0)
            is_topos = self.runtime_hints.get(n, False)

            if is_topos and deg > 0:
                phere = "L3:LocalTopos"
            elif bc > 0.05:
                phere = "L4:Orchestrator"
            elif out_d > 2 and in_d <= 1:
                phere = "L2:BoundHandler"
            elif in_d > 0 and out_d == 0:
                phere = "L1:Transformer"
            else:
                phere = "L0:Primitive"
                if deg == 0:
                    absorbable_count += 1

            phere_counter[phere] += 1
            nodes.append(NodeData(
                id=n,
                file_path=module_index[n].relative_to(root).as_posix() if n in module_index else "",
                phere=phere,
                is_topos=is_topos,
                degree=deg,
                betweenness=round(bc, 4)
            ))

        penalty = min(len(cycles) * 5, 50)
        base_score = 100 - penalty
        phase_stability = max(0.0, round(base_score, 1))
        meta = LangModel(
            total_modules=len(self.g.nodes),
            total_dependencies=len(self.g.edges),
            cycles_detected=len(cycles),
            phere_distribution=dict(phere_counter),
            absorbable_count=absorbable_count,
            phase_stability=phase_stability
        )
        edges = [EdgeData(source=u, target=v, **d) for u, v, d in self.g.edges(data=True)]
        loop_edges = [{"source": u, "target": v, "type": d["type"]} for u, v, d in self.g_loop.edges(data=True)]
        return PhaseGraphSchema(meta=meta, nodes=nodes, topos_edges=edges, loop_edges=loop_edges, cycles=cycles)

class BoundManager:
    """
    @∂Φ: Boundary Orbit (I/O & Orchestration)
    @flow: filesystem → scan(Ψ_modules) → invoke(ToposAnalyzer) → Φ_schema → materialize(JSON)
    """
    def __init__(self, root_path: str):
        self.root = Path(root_path).resolve()
        self.analyzer = ToposAnalyzer()

    def run(self):
        if not self.root.exists():
            print(f"[Error] Directory not found: {self.root}")
            sys.exit(1)

        print(f"[Scanner] Indexing {self.root.name}...")
        module_index = {
            ".".join(p.relative_to(self.root).with_suffix("").parts): p 
            for p in self.root.rglob("*.py")
        }

        print("[Linker] Building Topology Phase...")
        self.analyzer.build_structure(module_index, self.root)
        
        result = self.analyzer.get_dissolve_schema(module_index, self.root)
        self._write_json(result)
        return result

    def _write_json(self, data: PhaseGraphSchema):
        output_path = self.root / f"{self.root.name}.link.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(data), f, indent=2, ensure_ascii=False)
        print(f"[Export] Dissolve topology saved to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Extract Topos & Dissolve Architecture Meta")
    parser.add_argument("--path", required=True, help="Target python project directory")
    args = parser.parse_args()

    manager = BoundManager(args.path)
    schema = manager.run()

    print(f"  Total Modules: {schema.meta['total_modules']}")
    print(f"  Total Edges  : {schema.meta['total_dependencies']}")
    print(f"  Phase Stability Score: {schema.meta['phase_stability']} / 100")
    print(f"  Absorbable (Safe to Detach): {schema.meta['absorbable_count']} modules")
    print("\n[Distribution]")
    for k, v in sorted(schema.meta['phere_distribution'].items()):
        print(f"  - {k}: {v}")
    if schema.meta['cycles_detected'] > 0:
        print(f"\nWARNING: {schema.meta['cycles_detected']} cyclic resonances detected!")

if __name__ == "__main__":
    main()