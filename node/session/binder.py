# node.session.binder
"""
@flow: Scanner -> Tracer -> Registry -> Schema -> Interface
"""
import json
from pathlib import Path
from typing import Optional, Dict
from model.code.topic import TopicMap
from model.code.ext import ExtRegistry
from block.code.bounder import CodeBounder 
from block.code.inspect import ProjectMapper, ImportLens, IntegrityChecker

class FieldActivator:
    """[Phase 1] 환경 설정 및 위상 지도 로드 담당"""
    @staticmethod
    def activate(target_path: Path) -> Optional[TopicMap]:
        print(f"[Binder:Session] Field Activation: {target_path}")
        ProjectMapper.setup_workspace(target_path)
        
        repo_name = target_path.name
        topic_json = Path(f"xor/bound/{repo_name}.code.topic.json")
        
        if topic_json.exists():
            try:
                t_map = TopicMap.load_from_json(str(topic_json))
                print(f"[*] Topos Map Loaded: {len(t_map.phase_spaces)} Phases detected.")
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
        self.output_dir = Path("xor") / "bound" / repo_name
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
        gold = {k: v.model_dump() for k, v in self.hypotheses.items() if v.status == "Deep_Boundary_Mapped"}
        with open(self.output_dir / "gold_bounds.json", "w", encoding="utf-8") as f:
            json.dump(gold, f, indent=2, ensure_ascii=False)

    def _save_phase_shards(self):
        for pid, info in self.topic_map.phase_spaces.items():
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
        gold = len([v for v in hypotheses.values() if v.status == "Deep_Boundary_Mapped"])
        rate = (gold / total * 100) if total > 0 else 0

        print("\n" + "="*60)
        print(f"💡 [Emergence] Structural Evolution: {repo_name.upper()}")
        print("-" * 60)
        print(f"- Total Hypotheses : {total}")
        print(f"- Gold Bounds      : {gold} ({rate:.1f}%)")
        print(f"- Storage Path     : {output_path.absolute()}")
        print("=" * 60)

class SessionBinder:
    """[Orchestrator] Binder 실행의 전체 생명주기 관리"""
    def __init__(self, target_str: str):
        self.target_path = Path(target_str).resolve()
        self.repo_name = self.target_path.name
        self.topic_map = None
        self.binder = None

    def start(self):
        # 1. 활성화 (Activator)
        self.topic_map = FieldActivator.activate(self.target_path)

        # 2. 스캔 실행 (CodeBinder)
        self.binder = CodeBounder(topic_map=self.topic_map)
        self.binder.run_strategic_scan(self.target_path)

        # 3. 계층 저장 (Storage)
        storage = StratifiedStorage(
            self.repo_name, 
            self.binder.registry._hypotheses, 
            self.topic_map
        )
        output_path = storage.save_all()

        # 4. 결과 보고 (Reporter)
        EmergenceReporter.report(
            self.repo_name, 
            self.binder.registry._hypotheses, 
            output_path
        )

# 최종 진입점
def run(target_str: str):
    session = SessionBinder(target_str)
    session.start()

if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "."
    run(target)