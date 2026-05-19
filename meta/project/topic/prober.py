# meta.project.topic.prober
## @lineage: foldbox.actor.context.topic.prober
## @lineage: bridge.actor.context.topic.prober
## @lineage: center.actor.context.topic.prober
## @lineage: context.topic.prober
## @lineage: project.topic.prober
## @lineage: meta.project.code.prober
"""@flow: Scanner -> Tracer -> Registry -> Schema -> Interface"""
import sys
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, List
from phase.bound.resolver import find_current_self, resolve_path
from phase.plane.emitter import get_emitter
from arch.contract.registry.unified import contract
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from arch.project.topic.registry import TopicMap
from arch.project.topic.tracer import TopicTracer
from arch.project.topic.modeler import run_topos_clustering

log = get_emitter("topos.prober")
CODE_ROOT = resolve_path('code')

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
        log.info(f"[Binder:Session] Field Activation: {target_path}")
        ProjectMapper.setup_workspace(target_path)
        
        repo_name = target_path.name
        topic_json = CODE_ROOT / "topic" / f"{repo_name}.json"
        if not topic_json.exists():
            log.info(f"[*] TopicMap missing for '{repo_name}'. Triggering Topology Scanner...")
            try:
                run_topos_clustering(repo_name)
            except ImportError as e:
                log.info(f"[!] Failed to import Topic Engine: {e}")
                return None
            except Exception as e:
                log.info(f"[!] Topic clustering execution failed: {e}")
                return None
        
        if topic_json.exists():
            try:
                t_map = TopicMap.load_from_json(str(topic_json))
                log.info(f"[*] Topos Map Loaded: {len(t_map.spaces)} Phases detected.")
                return t_map
            except Exception as e:
                log.info(f"[!] Map Load Failure: {e}")
        return None

class StratifiedStorage:
    def __init__(self, repo_name: str, hypotheses: Dict, topic_map: Optional[TopicMap]):
        self.repo_name = repo_name
        self.hypotheses = hypotheses
        self.topic_map = topic_map
        self.output_dir = CODE_ROOT / "topic" / repo_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_all(self):
        self._save_index()
        self._save_map()
        if self.topic_map:
            self._save_architectural_shards() # 메서드명 및 로직 변경
        return self.output_dir

    def _save_index(self):
        index = {k: {"origin": v.module_origin, "state": v.status} for k, v in self.hypotheses.items()}
        with open(self.output_dir / "index.json", "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    def _save_map(self):
        bound = {k: v.model_dump() for k, v in self.hypotheses.items() if v.status == "bound.map"}
        with open(self.output_dir / "bounds.json", "w", encoding="utf-8") as f:
            json.dump(bound, f, indent=2, ensure_ascii=False)

    def _save_architectural_shards(self):
        """
        @flow: 5차원의 수학적 군집(Phase_X) -> 3차원의 인지적 위상 공간으로 붕괴(Collapse)
        """
        tiers = {
            "1_topos_core": {},    # 존재의 뼈대, 추상 규칙, 레지스트리
            "2_phase_flow": {},    # 동역학, 이벤트, 전이 상태
            "3_bound_surface": {}  # 물리적 집행, 외부 경계, I/O 센서
        }

        for pid, info in self.topic_map.spaces.items():
            core_paths = [m.path for m in info.core_modules]
            path_str = " ".join(core_paths).lower()

            # 1. 군집의 중심(Core) 성향을 파악하여 3대 공간 중 하나로 라우팅
            if any(k in path_str for k in ["topos", "meta", "registry", "schema", "contract"]):
                target_tier = "1_topos_core"
            elif any(k in path_str for k in ["phase", "flow", "loop", "event", "engine"]):
                target_tier = "2_phase_flow"
            else:
                target_tier = "3_bound_surface"

            # 2. 해당 군집에 속한 모듈 데이터를 목표 위상(Tier)에 병합(Merge)
            for k, v in self.hypotheses.items():
                if any(cp.replace('/', '.').replace('.py', '') in k for cp in core_paths):
                    tiers[target_tier][k] = v.model_dump()

        # 3. 최종적으로 3개의 명확한 파일로 디스크에 물리화(Physicalization)
        for tier_name, data in tiers.items():
            if data:  # 데이터가 존재하는 축만 파일로 생성
                with open(self.output_dir / f"{tier_name}.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

class EmergenceReporter:
    @staticmethod
    def report(repo_name: str, hypotheses: Dict, output_path: Path, ruptured_count: int = 0):
        total = len(hypotheses)
        bound = len([v for v in hypotheses.values() if v.status == "bound.map"])
        rate = (bound / total * 100) if total > 0 else 0

        log.info(f"## [Emergence] Structural Evolution: {repo_name.upper()}")
        log.info(f"- Total Hypotheses : {total}")
        log.info(f"- Bounds           : {bound} ({rate:.1f}%)")
        
        # [개선] 0개일지라도 무조건 위상 상태(Rupture 상태)를 콘솔에 증명하도록 변경
        if ruptured_count > 0:
            log.info(f"- 🛑 Ruptured      : {ruptured_count} Modules (Quarantined)")
        else:
            log.info(f"- 🟢 Ruptured      : 0 Modules (Topologically Safe)")
            
        log.info(f"- Storage Path     : {output_path.absolute()}")

class TopicProber:
    def __init__(self, target_repo: str):
        self.target_path = Path(target_repo).resolve()
        self.repo_name = self.target_path.name
        self.topic_map = None
        self.tracer = None # binder를 tracer로 명칭 수정

    def run(self, **kwargs):
        self.topic_map = FieldActivator.activate(self.target_path)
        if not self.topic_map:
            log.info("[!] Analysis aborted due to missing TopicMap.")
            return {"status": "fail", "reason": "Missing TopicMap"}

        # Tracer 생성 및 스캔
        self.tracer = TopicTracer(topic_map=self.topic_map)
        self.tracer.run_strategic_scan(self.target_path)

        # Storage 및 리포팅
        storage = StratifiedStorage(self.repo_name, self.tracer.registry._hypotheses, self.topic_map)
        output_path = storage.save_all()
        
        # Reporter에 순환 참조로 격리된 모듈 수 전달
        ruptured_set = getattr(self.tracer, 'ruptured_modules', set())
        ruptured_count = len(ruptured_set)
        EmergenceReporter.report(self.repo_name, self.tracer.registry._hypotheses, output_path, ruptured_count)
        
        return {
            "status": "success",
            "repo_name": self.repo_name,
            "topic_count": len(self.tracer.registry._hypotheses),
            "ruptured_modules": ruptured_count,
            "storage_path": str(output_path)
        }

def entry_task(args):
    """실행 인자를 파싱하고 태스크 어댑터를 반환하는 엔트리 포인트"""
    parser = argparse.ArgumentParser(description="Code Topology Binder Session")
    parser.add_argument("--repo", type=str, default=".", help="Target directory")
    parsed_args, _ = parser.parse_known_args(args)
    session = TopicProber(parsed_args.repo)
    return CliTaskAdapter(session.run)

@contract.cli(name="topic.prober", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("topic.prober", entry_task, __file__)

if __name__ == "__main__":
    main()