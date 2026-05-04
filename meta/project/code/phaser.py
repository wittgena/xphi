# meta.project.code.phaser
import argparse
import sys
import networkx as nx
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple
from bound.surface.emitter import get_logger
from bound.resolver import resolve_path
from bound.code.manager import CodeManager
from meta.project.code.analyzer import CodeAnalyzer

CODE_ROOT = resolve_path("code")
log = get_logger("code.phaser")

class CodePhaser:
    """
    @∂Φ: Boundary Orchestrator
    @flow: Analyzer의 살아있는 Φ_graph 주입 -> 위상 누수 감지(Leakage) -> 절단면(Cut-set) 계산 -> 독립 위상(Phase) 획정
    """
    PHERE_WEIGHT = {"L0": 0, "L1": 1, "L2": 2, "L3": 3, "L4": 4}

    def __init__(self, analyzer: CodeAnalyzer, repo_root: Path):
        self.analyzer = analyzer
        self.repo_root = repo_root
        self.g = analyzer.g  
        
        self.node_pheres = {
            n: self._extract_phase(n) for n in self.g.nodes
        }

    def _extract_phase(self, node: str) -> str:
        in_d = self.g.in_degree(node)
        out_d = self.g.out_degree(node)
        is_topos = self.analyzer.runtime_hints.get(node, False)
        
        if is_topos and self.g.degree(node) > 0: return "L3:LocalTopos"
        if out_d > 2 and in_d <= 1: return "L2:BoundHandler"
        if in_d > 0 and out_d == 0: return "L1:Transformer"
        return "L0:Primitive"

    def find_fracture_points(self, target_node: str) -> List[Dict[str, Any]]:
        closure = set(nx.descendants(self.g, target_node))
        closure.add(target_node)
        
        fractures = []
        for u, v in self.g.edges():
            if u in closure and v not in closure:
                fractures.append({
                    "from_internal": u,
                    "to_external": v,
                    "suggested_action": "Introduce Protocol/Interface"
                })
        return fractures

    def plan_topos_extraction(self) -> List[Dict[str, Any]]:
        plans = []
        target_nodes = [n for n, ph in self.node_pheres.items() if "L3" in ph or "L4" in ph]
        
        for root in target_nodes:
            closure_nodes = list(nx.descendants(self.g, root))
            closure_nodes.append(root)
            
            fractures = self.find_fracture_points(root)
            
            plans.append({
                "target_module": root,
                "phere": self.node_pheres[root],
                "closure_size": len(closure_nodes),
                "is_perfect_closure": len(fractures) == 0,
                "fracture_points": fractures,
                "closure_nodes": sorted(closure_nodes)
            })
            
        return sorted(plans, key=lambda x: (x["is_perfect_closure"], x["closure_size"]), reverse=True)

    def detect_layer_violations(self) -> List[Dict[str, str]]:
        violations = []
        for u, v in self.g.edges():
            u_weight = self.PHERE_WEIGHT.get(self.node_pheres[u][:2], 0)
            v_weight = self.PHERE_WEIGHT.get(self.node_pheres[v][:2], 0)
            
            if u_weight < v_weight:
                violations.append({
                    "violation_type": "Upward Dependency (Gravity Inversion)",
                    "from_module": u,
                    "from_phere": self.node_pheres[u],
                    "to_module": v,
                    "to_phere": self.node_pheres[v],
                })
        return violations


class PhaserEngine:
    """
    @engine: Topos Execution Engine (위상 전이 오케스트레이터)
    @flow: Φ_map(투영) -> ∂Φ_bound(경계 획정) -> Φ_emit(결과 방출)
    """
    def __init__(self, repo_path: str):
        self.repo_root = Path(repo_path).resolve()
        if not self.repo_root.exists():
            log.error(f"[Fatal] Target repository not found: {self.repo_root}")
            sys.exit(1)

    def _build_module_index(self) -> Dict[str, Path]:
        """물리적 파일 시스템을 위상 모듈 인덱스로 투영"""
        return {
            ".".join(p.relative_to(self.repo_root).with_suffix("").parts): p 
            for p in self.repo_root.rglob("*.py")
        }

    def execute(self):
        """전체 위상 분석 궤적 실행"""
        # Phase 1: 투영 및 장(Field) 구축
        log.info(f"[Phase 1: Map] Projecting Topos Field for {self.repo_root.name}...")
        manager = CodeManager(str(self.repo_root))
        manager.analyzer.build_structure(self._build_module_index(), self.repo_root)

        # Phase 2: 경계 획정 및 균열 지점 계산
        log.info("[Phase 2: Bound] Calculating Phase Boundaries & Fracture Points...")
        bounder = CodePhaser(manager.analyzer, self.repo_root)
        violations = bounder.detect_layer_violations()
        extraction_plans = bounder.plan_topos_extraction()

        # Phase 3: 분석 잔여물 방출
        self._emit_report(violations, extraction_plans)

    def _emit_report(self, violations: List[Dict], extraction_plans: List[Dict]):
        """관측 결과를 외부(CLI/Log)로 방출(Collapse)"""
        log.info("\n## Topos Dissolve & Boundary Report")
        
        if violations:
            log.warning(f"\n[!] Detected {len(violations)} Phase Violations (Upward Dependencies):")
            for v in violations:
                log.warning(f"  - {v['from_module']} ({v['from_phere']}) -> {v['to_module']} ({v['to_phere']})")
        
        log.info("\n[Top 3 Perfect Phase Extractions (Zero External Gravity)]")
        perfect_closures = [p for p in extraction_plans if p["is_perfect_closure"]]
        for p in perfect_closures[:3]:
            log.info(f"  - {p['target_module']} (Contains {p['closure_size']} pure sub-modules)")
            
        log.info("\n[Modules Requiring Protocol Abstraction (Fractured Closures)]")
        fractured_closures = [p for p in extraction_plans if not p["is_perfect_closure"]]
        for p in fractured_closures[:3]:
            log.info(f"  - {p['target_module']}: Needs {len(p['fracture_points'])} interfaces")
            for f in p['fracture_points'][:2]:
                log.info(f"      * Disconnect: {f['from_internal']} -/-> {f['to_external']}")


def main():
    """@axis: 실행의 진입점 (순수 선언적 궤도)"""
    parser = argparse.ArgumentParser(description="Topological Bounding & Phase Extraction")
    parser.add_argument("--repo", required=True, help="Target project directory")
    args = parser.parse_args()

    engine = PhaserEngine(args.repo)
    engine.execute()

if __name__ == "__main__":
    main()