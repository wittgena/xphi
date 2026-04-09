# node.topos.entry
import json
from pathlib import Path
from typing import Dict, Any
from surface.plane.emitter import get_logger
from anchor.resolver import resolve_path

MODEL_ROOT = resolve_path('model')

log = get_logger("model.renderer")

class SurfaceTemplate:
    """
    @role: template for ∂Φ (Boundary Surface)
    @meaning: The visual skeleton for human/system observation
    """
    MARKDOWN = """# Topological Surface: {entry_point}

> **Phase**: Φ′ → Φs
> **Role**: Boundary Projection

## @entry.interface
- **Origin Node**: `{entry_point}`
- **Projected Capabilities**: {projections}

---

## @topos.fragments (Minimal Units)
{fragments}

---

## @topos.resonance (Relations)
{relations}

---
*Projected via `signature.graph.renderer` (Static Boundary)*
"""

class SurfaceProjector:
    """
    @role: structural translator
    @mapping: Φ′ (Compiled Graph Dict) → Template Variables
    """
    @staticmethod
    def translate(compiled_graph: Dict[str, Any]) -> Dict[str, str]:
        entry_point = compiled_graph.get("entry_point", "Unknown")

        # 1. Projections Formatting
        projs = compiled_graph.get("projections", [])
        projections_str = ", ".join(f"`{p}`" for p in projs) if projs else "None"

        # 2. Fragments (Nodes) Formatting
        nodes = compiled_graph.get("nodes", {})
        fragments_blocks = []
        for f_id, frag in nodes.items():
            label = frag.get("label", "unknown")
            attrs = frag.get("attributes", {})
            # 속성들을 보기 좋게 문자열화 (빈 딕셔너리는 생략)
            attr_str = f" | attributes: {json.dumps(attrs, ensure_ascii=False)}" if attrs else ""
            fragments_blocks.append(f"- **`{f_id}`** (`{label}`){attr_str}")
        fragments_str = "\n".join(fragments_blocks) if fragments_blocks else "- (No fragments)"

        # 3. Relations (Edges) Formatting
        relations_blocks = []
        for f_id, frag in nodes.items():
            rels = frag.get("relations", [])
            for r in rels:
                target = r.get("target", r.get("dst", "unknown"))
                rel_type = r.get("rel", "links_to")
                relations_blocks.append(f"- `{f_id}` --[{rel_type}]--> `{target}`")
        relations_str = "\n".join(relations_blocks) if relations_blocks else "- (No relations)"

        return {
            "entry_point": entry_point,
            "projections": projections_str,
            "fragments": fragments_str,
            "relations": relations_str
        }

class BoundaryRenderer:
    """
    @phase: Φ′ → Φs compiler

    @topos.position:
      - Sits at the very edge (∂Φ) of the system.
      - Consumes static output from GraphCompiler.
      
    @topos.function:
      - Does not execute. Does not mutate.
      - Simply casts the structural shadow onto a readable surface (Markdown).
    """
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def project_dict(self, compiled_graph: Dict[str, Any], filename: str = "SURFACE.md") -> str:
        """메모리 상의 컴파일된 그래프 딕셔너리를 직접 투영"""
        log.info(f"Initiating boundary projection for entry: {compiled_graph.get('entry_point')}")

        surface_data = SurfaceProjector.translate(compiled_graph)
        surface_content = SurfaceTemplate.MARKDOWN.format(**surface_data)

        output_path = self.output_dir / filename
        output_path.write_text(surface_content, encoding="utf-8")

        log.info(f"[Φs] System surface projected to {output_path}")
        return surface_content

    def project_file(self, graph_file: Path) -> str:
        """디스크에 저장된 컴파일 산출물(JSON)을 읽어 투영"""
        if not graph_file.exists():
            raise FileNotFoundError(f"Compiled graph not found: {graph_file}")
        
        with open(graph_file, "r", encoding="utf-8") as f:
            compiled_graph = json.load(f)
            
        out_name = f"{graph_file.stem}_surface.md"
        return self.project_dict(compiled_graph, filename=out_name)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Boundary Projection Device (Φ′ → Φs)")
    parser.add_argument("--graph", required=True, help="Path to the compiled graph JSON file (Φ′)")
    parser.add_argument("--outdir", default=".", help="Directory to save the projected markdown")
    args = parser.parse_args()

    renderer = BoundaryRenderer(Path(args.outdir).resolve())
    renderer.project_file(Path(args.graph).resolve())