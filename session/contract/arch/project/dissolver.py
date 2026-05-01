# session.contract.arch.project.dissolver
import json
import argparse
import sys
import networkx as nx
from pathlib import Path
from typing import Dict, Any, List
from meta.flow.surface.emitter import get_logger
from session.bound.resolver import resolve_path

OUT_ROOT = resolve_path("topos")

log = get_logger("topos.dissolver")

class ToposDissolver:
    PHERE_WEIGHT = {"L0": 0, "L1": 1, "L2": 2, "L3": 3, "L4": 4}
    def __init__(self, link_data: Dict[str, Any]):
        self.meta = link_data.get("meta", {})
        self.nodes = {n["id"]: n for n in link_data.get("nodes", [])}
        
        self.g = nx.DiGraph()
        for node_id in self.nodes:
            self.g.add_node(node_id)
            
        for edge in link_data.get("topos_edges", []):
            self.g.add_edge(edge["source"], edge["target"])

    def harvest_safe_primitives(self) -> List[str]:
        """외부/내부 의존성이 전혀 없는 완벽한 고립 노드(L0)"""
        safe_files = []
        for n_id, data in self.nodes.items():
            if "L0" in data.get("layer", "") and self.g.degree(n_id) == 0:
                file_path = data.get("file_path")
                if file_path:
                    safe_files.append(file_path)
        return safe_files

    def plan_topos_extraction(self) -> List[Dict[str, Any]]:
        """특정 비즈니스 로직(L3/L4)의 하위 종속 파일들(Closure)의 집합을 계산.
        """
        plans = []
        target_nodes = [
            n_id for n_id, data in self.nodes.items() 
            if any(lvl in data.get("layer", "") for lvl in ["L3", "L4"])
        ]
        
        for root_node in target_nodes:
            # nx.descendants: root_node가 의존하는(import하는) 모든 하위 노드들을 재귀적으로 탐색
            closure_nodes = list(nx.descendants(self.g, root_node))
            closure_nodes.append(root_node) # 자기 자신 포함
            
            # 물리적 파일 경로로 변환
            required_files = [
                self.nodes[n]["file_path"] for n in closure_nodes 
                if n in self.nodes and self.nodes[n].get("file_path")
            ]
            
            if required_files:
                plans.append({
                    "target_module": root_node,
                    "layer": self.nodes[root_node]["layer"],
                    "closure_size": len(required_files),
                    "required_files": sorted(required_files)
                })
                
        # 덩치가 큰 모듈(같이 가져갈 파일이 많은 순)부터 내림차순 정렬
        return sorted(plans, key=lambda x: x["closure_size"], reverse=True)

    def detect_layer_violations(self) -> List[Dict[str, str]]:
        """L0나 L1 같은 하위 유틸리티가 L3, L4의 도메인 로직을 역참조하는지 감시"""
        violations = []
        for u, v in self.g.edges():
            u_layer_prefix = self.nodes.get(u, {}).get("layer", "L0")[:2]
            v_layer_prefix = self.nodes.get(v, {}).get("layer", "L0")[:2]
            
            w_u = self.PHERE_WEIGHT.get(u_layer_prefix, 0)
            w_v = self.PHERE_WEIGHT.get(v_layer_prefix, 0)
            
            # 하위 레이어가 상위 레이어를 참조한 경우 (역방향 참조)
            if w_u < w_v:
                violations.append({
                    "violation_type": "Upward Dependency",
                    "from_module": u,
                    "from_layer": self.nodes[u].get("layer"),
                    "to_module": v,
                    "to_layer": self.nodes[v].get("layer"),
                })
        return violations

    def run(self) -> Dict[str, Any]:
        return {
            "meta": {
                "source_project_modules": self.meta.get("total_modules", 0),
                "phase_stability": self.meta.get("phase_stability", 0.0)
            },
            "layer_violations": self.detect_layer_violations(),
            "safe_primitives_to_harvest": self.harvest_safe_primitives(),
            "topos_extraction_plans": self.plan_topos_extraction()
        }

def main():
    parser = argparse.ArgumentParser(description="Generate Absorption/Extraction Plan from Topos Map")
    parser.add_argument("--repo", required=True, help="Target project directory")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    project_name = repo.name
    
    link_file = OUT_ROOT / "link" / f"{project_name}.json"
    if not link_file.exists():
        log.error(f"[Error] Topos map not found. Run linker first: {link_file}")
        sys.exit(1)

    with open(link_file, "r", encoding="utf-8") as f:
        link_data = json.load(f)

    planner = ToposDissolver(link_data)
    plan_report = planner.run()

    bound_file = repo / "bound" / f"{project_name}.json"
    with open(bound_file, "w", encoding="utf-8") as f:
        json.dump(plan_report, f, indent=2, ensure_ascii=False)

    print("##  Project Dissolver")
    violations = len(plan_report['layer_violations'])
    primitives = len(plan_report['safe_primitives_to_harvest'])
    plans = len(plan_report['topos_extraction_plans'])
    
    print(f"- Layer Violations Detected: {violations} (Needs fix before clean extraction)")
    print(f"- Safe Primitives (Ready to copy): {primitives} files")
    print(f"- Topos Extraction Plans: {plans} logic blocks identified")
    
    if plans > 0:
        print("\n[Top 3 Heaviest Modules to Extract]")
        for p in plan_report['topos_extraction_plans'][:3]:
            print(f"  - {p['target_module']} (Requires {p['closure_size']} files)")
            
    print(f"\n[Export] Plan saved to: {bound_file}")

if __name__ == "__main__":
    main()