# node.surface.entry
import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List
from plane.emitter import get_logger
from anchor.resolver import resolve_path, get_invoker
from node.model.schema import ToposGraph, ToposNode
from node.model.binder import ModelBinder
from contract.registry import cli_contract
from bridge.executor.cli import execute_cli_task, CliTaskAdapter

log = get_logger("surface.entry")

class SurfaceTemplate:
    MARKDOWN = """# Entry: {entry_point}

> **Phase**: Φ′ → Ψ (Contextual Subgraph) → Φs
> **Role**: Dynamic Perspective Projection

## @focus.context
- **Focus**: `{focus}`
- **Depth**: {depth}
- **Relations**: {relations_list}

---

## @local.topology (Selected Fragments)
{fragments}

---

## @expansion (Paths)
{relations}

---
*Projected via `EntryProjector` (Dynamic Boundary)*
"""

class EntrySelector:
    @staticmethod
    def select(graph_data: dict, context: dict) -> ToposGraph:
        full_nodes = {k: ToposNode(**v) for k, v in graph_data.get("nodes", {}).items()}
        
        entry_id = context.get("entry")
        depth = context.get("depth", 1)
        valid_relations = set(context.get("relations", ["coupled"]))

        if entry_id not in full_nodes:
            log.warning(f"Entry point '{entry_id}' not found in the graph.")
            return ToposGraph(invariants=[], nodes={})

        selected_ids = {entry_id}
        current_level = {entry_id}
        
        for _ in range(depth):
            next_level = set()
            for n_id in current_level:
                for rel in full_nodes[n_id].relations:
                    rel_type = rel.get("rel") if isinstance(rel, dict) else rel.rel
                    rel_target = rel.get("target") if isinstance(rel, dict) else rel.target
                    
                    if rel_type in valid_relations and rel_target in full_nodes:
                        next_level.add(rel_target)
            selected_ids.update(next_level)
            current_level = next_level

        sub_nodes = {}
        for n_id in selected_ids:
            node = full_nodes[n_id]
            filtered_rels = []
            for r in node.relations:
                r_type = r.get("rel") if isinstance(r, dict) else r.rel
                r_target = r.get("target") if isinstance(r, dict) else r.target
                if r_target in selected_ids and r_type in valid_relations:
                    filtered_rels.append(r)
            
            sub_nodes[n_id] = ToposNode(
                id=node.id, intensity=node.intensity, is_invariant=node.is_invariant,
                boundaries=node.boundaries, support_manifold=node.support_manifold,
                relations=filtered_rels
            )

        return ToposGraph(invariants=[], nodes=sub_nodes)

class SurfaceProjector:
    @staticmethod
    def translate(subgraph: ToposGraph, context: dict) -> dict:
        entry_point = context.get("entry", "Unknown")

        # 1. Fragments (Nodes) Formatting
        fragments_blocks = []
        for f_id, node in subgraph.nodes.items():
            intensity = node.intensity
            boundaries = json.dumps(node.boundaries, ensure_ascii=False)
            inv_mark = " *(Invariant)*" if node.is_invariant else ""
            
            prefix = "**[ENTRY]** " if f_id == entry_point else "- "
            fragments_blocks.append(f"{prefix}**`{f_id}`**{inv_mark} | Intensity: {intensity} | Boundaries: {boundaries}")
            
        fragments_str = "\n".join(fragments_blocks) if fragments_blocks else "- (No fragments)"

        # 2. Relations (Edges) Formatting - Top 7 x 7 필터링 적용
        relations_blocks = []
        
        # 2-1. 노드별 전체 관계 강도(strength) 합산을 계산하는 헬퍼 함수
        def get_total_strength(n: ToposNode) -> int:
            return sum(r.get("strength", 1) if isinstance(r, dict) else r.strength for r in n.relations)

        # 2-2. 관계 강도 총합이 가장 높은 상위 7개 노드 추출
        top_source_nodes = sorted(
            subgraph.nodes.items(),
            key=lambda item: get_total_strength(item[1]),
            reverse=True
        )[:7]

        # 2-3. 선택된 각 노드에 대해 상위 7개 관계만 추출
        for f_id, node in top_source_nodes:
            # 개별 엣지를 strength 기준으로 내림차순 정렬하여 상위 7개 선택
            top_relations = sorted(
                node.relations,
                key=lambda r: r.get("strength", 1) if isinstance(r, dict) else r.strength,
                reverse=True
            )[:7]

            for rel in top_relations:
                rel_type = rel.get("rel") if isinstance(rel, dict) else rel.rel
                rel_target = rel.get("target") if isinstance(rel, dict) else rel.target
                rel_strength = rel.get("strength", 1) if isinstance(rel, dict) else rel.strength
                
                relations_blocks.append(f"- `{f_id}` --[{rel_type} (str: {rel_strength})]--> `{rel_target}`")
                
        relations_str = "\n".join(relations_blocks) if relations_blocks else "- (No relations within this scope)"

        return {
            "entry_point": entry_point,
            "focus": context.get("focus", "general"),
            "depth": str(context.get("depth", 0)),
            "relations_list": ", ".join(f"`{r}`" for r in context.get("relations", [])),
            "fragments": fragments_str,
            "relations": relations_str
        }

class ContextualEntryProjector:
    def __init__(self, context: dict):
        self.context = context

    def project(self, compiled_graph: dict) -> str:
        selector = EntrySelector()
        subgraph = selector.select(compiled_graph, self.context)
        surface_data = SurfaceProjector.translate(subgraph, self.context)
        return SurfaceTemplate.MARKDOWN.format(**surface_data)

class BoundRenderer:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def project_file(self, graph_file: Path, context: Dict[str, Any]) -> str:
        with open(graph_file, "r", encoding="utf-8") as f:
            compiled_graph = json.load(f)
            
        log.info(f"Initiating contextual projection for entry: {context.get('entry')}")
        projector = ContextualEntryProjector(context)
        surface_content = projector.project(compiled_graph)

        out_name = f"{context.get('entry')}.surface.md"
        output_path = self.output_dir / out_name
        output_path.write_text(surface_content, encoding="utf-8")

        log.info(f"[Φs] Contextual surface projected to {output_path}")
        return surface_content


# ---------------------------------------------------------
# 2. 실행 파이프라인 (Orchestrator) 캡슐화
# ---------------------------------------------------------
class SurfaceEntryOrchestrator:
    """
    Auto-Binding 과 Context Projection을 하나로 묶어 CliTaskAdapter에 주입하기 위한 래퍼 클래스
    """
    def __init__(self, entry: str, target_dir: Optional[str] = None, rebuild: bool = False,
                 depth: int = 1, relations: Optional[List[str]] = None, outdir: str = "."):
        self.entry = entry
        self.target_dir = target_dir
        self.rebuild = rebuild
        self.depth = depth
        self.relations = relations or ["coupled"]
        self.outdir = resolve_path("surface") 
        self.fixed_graph_path = resolve_path("xor") / "node" / "model.bound.json"

    def execute(self):
        # 1. Auto-Bind 로직 (그래프 없으면 생성)
        if not self.fixed_graph_path.exists() or self.rebuild:
            log.info(f"[Auto-Bind] Graph topology missing or rebuild requested. Orchestrating ModelBinder...")
            binder = ModelBinder()
            if self.target_dir:
                binder.model_root = resolve_path('model') / self.target_dir
                log.info(f"[Auto-Bind] Focusing bind scope to directory: {binder.model_root}")
            else:
                log.info("[Auto-Bind] Binding entire model manifold...")
            binder.execute()
            log.info("[Auto-Bind] Topology generation complete.")

        # 2. 파이프라인 컨텍스트 정의
        execution_context = {
            "focus": "CLI Observation",
            "entry": self.entry,
            "depth": self.depth,
            "relations": self.relations
        }

        # 3. 렌더링 파이프라인 실행
        renderer = BoundRenderer(self.outdir)
        return renderer.project_file(self.fixed_graph_path, execution_context)


@cli_contract(name="surface.entry", args=["--entry", "self"], tags=["surface"], recept=["node.model.binder"])
def main():
    parser = argparse.ArgumentParser(description="Contextual Boundary Projection (Φ′ + Context → Ψ → Φs)")
    parser.add_argument("--entry", required=True, help="Entry node ID (e.g., 특정 개념어나 클래스명)")
    parser.add_argument("--dir", default=None, help="Target model directory (지정 안 하면 전체 model 스캔)")
    parser.add_argument("--rebuild", action="store_true", help="Force rebuild the graph topology via binder")
    parser.add_argument("--depth", type=int, default=1, help="Graph expansion depth (기본: 1)")
    parser.add_argument("--relations", nargs='+', default=["coupled"], help="Relation types to follow")
    parser.add_argument("--outdir", default=".", help="Directory to save the projected markdown")
    
    args = parser.parse_args()

    ## Orchestrator 객체 생성
    orchestrator = SurfaceEntryOrchestrator(
        entry=args.entry,
        target_dir=args.dir,
        rebuild=args.rebuild,
        depth=args.depth,
        relations=args.relations,
        outdir=args.outdir
    )

    ## Bridge Executor 패턴 연동
    module_name = __name__ if __name__ != "__main__" else "surface.entry"
    task = CliTaskAdapter(orchestrator.execute)
    
    invoker, command = get_invoker(Path(__file__))
    payload = {"_context": {"invoker": str(invoker), "command": command, "cli_args": sys.argv[1:]}}
    
    execute_cli_task(task_instance=task, command_name=module_name, payload=payload)

if __name__ == "__main__":
    log.info(f"[AUG] node model surface entry :: sys.argv = {sys.argv}")
    main()