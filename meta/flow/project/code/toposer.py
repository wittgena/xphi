# foldbox.loop.lang.topic.mapper
"""
@desc: Topological Boundary Engine (위상 경계 획정 엔진)
@flow: Φ_map(투영) → Φ_activate(장 활성화) → ∂Φ_bound(경계 획정) → Φ_emit(잔여물 방출)
"""
import sys
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, List
from bound.code.tracer import CodeTracer 
from topos.project.code.topic.map import TopicMap
from bound.resolver import find_current_self, resolve_path, get_invoker
from phase.node.executor.cli import execute_cli_task, CliTaskAdapter
from bound.surface.emitter import get_logger

XOR_ROOT = resolve_path('xor')
log = get_logger("code.toposer")

class BoundaryProjector:
    """
    @phase: Map (투영)
    @desc: 물리적 파일 시스템을 위상 공간으로 끌어올리기 위한 기초 앵커링
    """
    @staticmethod
    def inject_workspace(target_path: Path) -> str:
        base_dir = str(target_path if target_path.is_dir() else target_path.parent)
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        return base_dir


class ToposActivator:
    """
    @phase: Activate (장 활성화)
    @desc: TopicMap을 로드하여 잠들어있는 위상 지도를 활성화 (Resonance)
    """
    @staticmethod
    def awaken(target_path: Path) -> Optional[TopicMap]:
        log.info(f"[Phase:Activate] Awakening Field: {target_path.name}")
        BoundaryProjector.inject_workspace(target_path)
        
        topic_json = XOR_ROOT / "bound" / f"{target_path.name}.code.topic.json"
        
        if not topic_json.exists():
            log.warning(f"[*] TopicMap missing. Topology Scanner must be triggered manually or upstream.")
            # Note: run_clustering_for_repo 호출 로직은 외부 의존성이므로 방어적으로 처리
            return None
        
        try:
            t_map = TopicMap.load_from_json(str(topic_json))
            log.info(f"[*] Topos Map Loaded: {len(t_map.phase_spaces)} Phase Spaces resonating.")
            return t_map
        except Exception as e:
            log.error(f"[!] Phase Field Collapse Failure: {e}")
            return None


class PhaseResidueStorage:
    """
    @phase: Emit (붕괴 및 방출)
    @desc: 메모리에 떠있는 가변적인 가설(Hypotheses)들을 불변의 물리적 JSON (Residue)으로 박제
    """
    def __init__(self, repo_name: str, hypotheses: Dict, topic_map: Optional[TopicMap]):
        self.output_dir = XOR_ROOT / "bound" / repo_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.hypotheses = hypotheses
        self.topic_map = topic_map

    def collapse_to_disk(self) -> Path:
        """위상 상태를 물리적 매질로 붕괴(Collapse)"""
        self._emit_index()
        self._emit_gold_bounds()
        if self.topic_map:
            self._emit_phase_shards()
        return self.output_dir

    def _emit_index(self):
        index = {k: {"origin": v.module_origin, "state": v.status} for k, v in self.hypotheses.items()}
        with open(self.output_dir / "index.json", "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    def _emit_gold_bounds(self):
        gold = {k: v.model_dump() for k, v in self.hypotheses.items() if v.status == "deep.bound.map"}
        with open(self.output_dir / "gold.bounds.json", "w", encoding="utf-8") as f:
            json.dump(gold, f, indent=2, ensure_ascii=False)

    def _emit_phase_shards(self):
        """독립된 Phase(닫힌 계) 별로 파편화하여 방출"""
        for pid, info in self.topic_map.phase_spaces.items():
            core_paths = [m.path for m in info.core_modules]
            phase_data = {
                k: v.model_dump() for k, v in self.hypotheses.items()
                if any(cp.replace('/', '.').replace('.py', '') in k for cp in core_paths)
            }
            if phase_data:
                with open(self.output_dir / f"{pid}.json", "w", encoding="utf-8") as f:
                    json.dump(phase_data, f, indent=2, ensure_ascii=False)


class CodeToposEngine:
    """
    @engine: Topos Execution Engine (위상 전이 오케스트레이터)
    """
    def __init__(self, target_repo: str):
        self.target_path = Path(target_repo).resolve()
        self.repo_name = self.target_path.name

    def execute(self, **kwargs) -> Dict:
        # 1. 장 활성화 (Field Activation)
        topic_map = ToposActivator.awaken(self.target_path)
        if not topic_map:
            return {"status": "fail", "reason": "Missing TopicMap"}

        # 2. 위상 경계 획정 (Topological Bounding)
        log.info("[Phase:Bound] Calculating Phase Boundaries...")
        tracer = CodeTracer(topic_map=topic_map)
        tracer.run_strategic_scan(self.target_path)
        hypotheses = tracer.registry._hypotheses

        # 3. 잔여물 방출 및 물리적 붕괴 (Collapse & Emit)
        storage = PhaseResidueStorage(self.repo_name, hypotheses, topic_map)
        output_path = storage.collapse_to_disk()

        # 4. 결과 리포팅 (Reporting)
        self._report_emergence(hypotheses, output_path)

        return {
            "status": "success",
            "repo_name": self.repo_name,
            "hypotheses_count": len(hypotheses),
            "storage_path": str(output_path)
        }

    def _report_emergence(self, hypotheses: Dict, output_path: Path):
        total = len(hypotheses)
        gold = len([v for v in hypotheses.values() if v.status == "deep.bound.map"])
        rate = (gold / total * 100) if total > 0 else 0

        log.info(f"\n## [Emergence] Structural Evolution: {self.repo_name.upper()}")
        log.info(f"  - Total Hypotheses : {total}")
        log.info(f"  - Gold Bounds      : {gold} ({rate:.1f}%)")
        log.info(f"  - Storage Path     : {output_path.absolute()}")


def main():
    """@axis: 순수 선언적 실행 궤도"""
    parser = argparse.ArgumentParser(description="Topological Phase Binder")
    parser.add_argument("--repo", type=str, default=".", help="Target directory to bound")
    args, _ = parser.parse_known_args()

    # 엔진 인스턴스화
    engine = CodeToposEngine(args.repo)
    
    # CLI 어댑터 매핑
    adapted_task = CliTaskAdapter(engine.execute)
    invoker, command = get_invoker(Path(__file__))
    
    payload = {
        "_context": {
            "invoker": str(invoker), 
            "command": command, 
            "cli_args": sys.argv[1:]
        }
    }
    
    execute_cli_task(task_instance=adapted_task, command_name="code.toposer", payload=payload)

if __name__ == "__main__":
    main()