# meta.project.code.topicer
"""@flow: Scanner -> Tracer -> Registry -> Schema -> Interface"""
import sys
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, List
from bound.resolver import find_current_self, resolve_path, get_invoker
from bound.code.schema import HypoRegistry
from bound.code.tracer import CodeTracer 
from phase.node.executor.cli import execute_cli_task, CliTaskAdapter
from topos.project.code.topic.map import TopicMap
from meta.project.topic.modeler import run_topos_clustering

XOR_ROOT = resolve_path('xor')

class ProjectMapper:
    """프로젝트의 위상과 경로를 관리합니다."""
    @staticmethod
    def setup_workspace(target_path: Path):
        """임포트가 가능하도록 최상위 경로를 sys.path에 등록합니다."""
        base_dir = str(target_path if target_path.is_dir() else target_path.parent)
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        return base_dir

    @staticmethod
    def collect_python_files(target_path: Path) -> List[Path]:
        """분석 대상 파일들을 수집합니다."""
        if target_path.is_file():
            return [target_path]
        return list(target_path.rglob("*.py"))

class FieldActivator:
    """[Phase 1] 환경 설정 및 위상 지도 로드 (자동화 적용)"""
    @staticmethod
    def activate(target_path: Path) -> Optional[TopicMap]:
        print(f"[Binder:Session] Field Activation: {target_path}")
        ProjectMapper.setup_workspace(target_path)
        
        repo_name = target_path.name
        topic_json = XOR_ROOT / "bound" / f"{repo_name}.code.topic.json"
        if not topic_json.exists():
            print(f"[*] TopicMap missing for '{repo_name}'. Triggering Topology Scanner...")
            try:
                run_topos_clustering(repo_name)
            except ImportError as e:
                print(f"[!] Failed to import Topic Engine: {e}")
                return None
            except Exception as e:
                print(f"[!] Topic clustering execution failed: {e}")
                return None
        
        if topic_json.exists():
            try:
                t_map = TopicMap.load_from_json(str(topic_json))
                print(f"[*] Topos Map Loaded: {len(t_map.spaces)} Phases detected.")
                return t_map
            except Exception as e:
                print(f"[!] Map Load Failure: {e}")
        return None

class StratifiedStorage:
    """[Phase 2] 계층적 데이터 저장 담당 (Index, Gold, Phase)"""
    def __init__(self, repo_name: str, hypotheses: Dict, topic_map: Optional[TopicMap]):
        self.repo_name = repo_name
        self.hypotheses = hypotheses
        self.topic_map = topic_map
        self.output_dir = XOR_ROOT / "bound" / repo_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_all(self):
        """모든 계층의 데이터를 물리적으로 박제"""
        self._save_index()
        self._save_gold_bounds()
        if self.topic_map:
            self._save_phase_shards()
        return self.output_dir

    def _save_index(self):
        index = {k: {"origin": v.module_origin, "state": v.status} for k, v in self.hypotheses.items()}
        with open(self.output_dir / "index.json", "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    def _save_gold_bounds(self):
        gold = {k: v.model_dump() for k, v in self.hypotheses.items() if v.status == "deep.bound.map"}
        with open(self.output_dir / "gold.bounds.json", "w", encoding="utf-8") as f:
            json.dump(gold, f, indent=2, ensure_ascii=False)

    def _save_phase_shards(self):
        for pid, info in self.topic_map.spaces.items():
            core_paths = [m.path for m in info.core_modules]
            phase_data = {
                k: v.model_dump() for k, v in self.hypotheses.items()
                if any(cp.replace('/', '.').replace('.py', '') in k for cp in core_paths)
            }
            if phase_data:
                with open(self.output_dir / f"{pid}.json", "w", encoding="utf-8") as f:
                    json.dump(phase_data, f, indent=2, ensure_ascii=False)

class EmergenceReporter:
    """[Phase 3] 분석 결과 요약 리포팅 담당"""
    @staticmethod
    def report(repo_name: str, hypotheses: Dict, output_path: Path):
        total = len(hypotheses)
        gold = len([v for v in hypotheses.values() if v.status == "deep.bound.map"])
        rate = (gold / total * 100) if total > 0 else 0

        print(f"## [Emergence] Structural Evolution: {repo_name.upper()}")
        print(f"- Total Hypotheses : {total}")
        print(f"- Gold Bounds      : {gold} ({rate:.1f}%)")
        print(f"- Storage Path     : {output_path.absolute()}")

class ProjectBinder:
    def __init__(self, target_repo: str):
        self.target_path = Path(target_repo).resolve()
        self.repo_name = self.target_path.name
        self.topic_map = None
        self.binder = None

    def run(self, **kwargs):
        self.topic_map = FieldActivator.activate(self.target_path)
        if not self.topic_map:
            print("[!] Analysis aborted due to missing TopicMap.")
            return {"status": "fail", "reason": "Missing TopicMap"}

        self.binder = CodeTracer(topic_map=self.topic_map)
        self.binder.run_strategic_scan(self.target_path)

        storage = StratifiedStorage(self.repo_name, self.binder.registry._hypotheses, self.topic_map)
        output_path = storage.save_all()
        EmergenceReporter.report(self.repo_name, self.binder.registry._hypotheses, output_path)
        return {
            "status": "success",
            "repo_name": self.repo_name,
            "hypotheses_count": len(self.binder.registry._hypotheses),
            "storage_path": str(output_path)
        }

def main():
    parser = argparse.ArgumentParser(description="Code Topology Binder Session")
    parser.add_argument("--repo", type=str, default=".", help="Target directory")
    args, _ = parser.parse_known_args()

    session = ProjectBinder(args.repo)
    adapted_task = CliTaskAdapter(session.run)
    invoker, command = get_invoker(Path(__file__))
    payload = {
        "_context": {
            "invoker": str(invoker), 
            "command": command, 
            "cli_args": sys.argv[1:]
        }
    }
    execute_cli_task(task_instance=adapted_task, command_name="code.binder", payload=payload)

if __name__ == "__main__":
    main()
