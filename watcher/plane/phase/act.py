# xphi.watcher.plane.phase.act
"""
@desc: 
- Infrastructure Orchestrator for CI/CD Plane (GitHub Actions via Nektos/act).
- Automates job execution, environment injection, and artifact determinism audits.
- Enforces strict remote isolation (no local bind) while extracting artifacts to /tmp.
"""
import os
import sys
import shutil
import platform
import asyncio
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Tuple

from xphi.arch.dev.tracer.base import SystemBound
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("plane.phase.act")

class ActBlueprint:
    @staticmethod
    def get_ci_spec() -> Dict[str, Dict[str, Any]]:
        """CI 파이프라인의 각 Job에 주입할 환경(Env)과 목표를 정의합니다."""
        return {
            "integration-test": {
                "env": {}, # CASE D (Remote Fallback): 로컬 바인딩 없이 원격 코드 통합 테스트
                "description": "Remote source binding validation (Isolated)"
            },
            "build-release": {
                "env": {"FIBER_BUILD_DIST": "1"}, # CASE A: 배포용 원격 강제 바인딩
                "description": "Deterministic remote binding for Wheel artifact"
            }
        }

class BaseActAdapter:
    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        raise NotImplementedError

class NektosActAdapter(BaseActAdapter):
    def __init__(self, workspace: Path, boundary: SystemBound, artifact_dir: Path):
        self.workspace = workspace
        self.boundary = boundary
        self.artifact_dir = artifact_dir
        
        if not shutil.which("act"):
            raise RuntimeError("[CRITICAL] 'act' binary not found. Standard CI emulation unavailable.")

    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        log.info(f"[Adapter:ACT] Provisioning Runner for Job: {job_name}")
        
        # [핵심 패치] --bind 제거 (로컬 오염 방지), 대신 artifact-server를 구동하여 결과물만 /tmp로 추출
        cmd = [
            "act", "-j", job_name, 
            "--artifact-server-path", str(self.artifact_dir)
        ]
        
        # Apple Silicon(M1/M2/M3) 아키텍처 충돌 방어
        if sys.platform == "darwin" and platform.machine() == "arm64":
            cmd.extend(["--container-architecture", "linux/amd64"])
            log.info("  ├─ Apple Silicon detected. Enforcing linux/amd64 architecture.")

        # GitHub Token 주입 (Rate Limit 및 actions/checkout Auth 에러 방지)
        gh_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if gh_token:
            cmd.extend(["-s", f"GITHUB_TOKEN={gh_token}"])
            log.info("  ├─ GITHUB_TOKEN secret securely injected.")
        else:
            log.warning("  ├─ ⚠️ GITHUB_TOKEN not found in env. Clone operations might fail due to rate limits.")

        for k, v in env.items():
            cmd.extend(["--env", f"{k}={v}"])
            log.info(f"  ├─ Injecting Env: {k}={v}")

        # act의 출력은 실시간으로 보여야 하므로 capture=False
        code, _, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        
        if code != 0:
            log.error(f"[Adapter:ACT] Job '{job_name}' execution fractured.")
            return False
            
        return True

class DeterminismAuditor:
    """빌드된 패키지(Artifact)의 의존성 무결성을 뜯어보는 감사관"""
    def __init__(self, target_dir: Path, boundary: SystemBound):
        self.target_dir = target_dir
        self.boundary = boundary
        self.is_clean = False

    def attach(self):
        pass 

    def detach(self):
        pass

    async def verify(self) -> bool:
        log.info(f"[Auditor:Determinism] Inspecting extracted Artifacts in {self.target_dir}...")
        
        # act가 임시 폴더 내 어느 하위 경로에 저장할지 모르므로 재귀 탐색(rglob) 수행
        wheel_files = list(self.target_dir.rglob("fiber-*.whl"))
        
        if not wheel_files:
            log.error("  └─ 💥 Artifact missing. Build phase did not produce or upload a Wheel.")
            log.warning("     (Did you include 'actions/upload-artifact' in your build.yml?)")
            return False
            
        target_wheel = wheel_files[0]
        cmd = f"unzip -p {target_wheel} fiber-*/METADATA | grep 'Requires-Dist: xphi'"
        
        code, out, _ = await self.boundary.run_command(["bash", "-c", cmd], capture=True)
        
        if code != 0:
            log.error("  └─ 💥 Metadata extraction failed. Zip structure might be corrupted.")
            return False
            
        metadata_line = out.strip()
        log.info(f"  ├─ Target Wheel: {target_wheel.name}")
        log.info(f"  ├─ Extracted Dependency: {metadata_line}")
        
        if "file://" in metadata_line:
            log.error("  └─ 💥 FATAL BREACH: Local absolute path leaked into Distribution Artifact!")
            self.is_clean = False
            return False
            
        log.info("  └─ ✨ AUDIT PASSED: Remote Binding Enforced mathematically.")
        self.is_clean = True
        return True

@dataclass
class ActContext:
    boundary: SystemBound
    adapter: BaseActAdapter  # E2E Scene에서 어댑터를 제어할 수 있도록 노출
    auditors: Dict[str, Any]

class ActOrchestrator:
    """ToposOrchestrator와 구조적 동형성(Isomorphism)을 완벽히 유지하는 CI 제어기"""
    def __init__(self, mode: str = "dev", suites: Dict[str, Any] = None):
        self.mode = mode
        self.suites = suites or {}
        self.keep_workspace = False
        
        self.workspace = Path.cwd()
        self.boundary = SystemBound()
        
        # [핵심] 호스트 OS의 /tmp 하위에 격리된 아티팩트 서버 경로 동적 생성
        self.artifact_dir = Path(tempfile.mkdtemp(prefix="fiber-act-artifacts-"))
        
        self.adapter = NektosActAdapter(self.workspace, self.boundary, self.artifact_dir)
        self.determinism_auditor = DeterminismAuditor(self.artifact_dir, self.boundary)
        
        # 레포팅(ToposFlow/ActFlow)을 위해 실행된 suite 인스턴스들을 추적
        self.suite_runners = {}

    async def _run_all_suites(self, broker: Any, context: ActContext) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Executing CI Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker=broker, context=context)
                self.suite_runners[suite_name] = suite_instance
                
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating CI/CD Plane via Nektos/act ({self.mode.upper()}) ---")
        log.info(f"[SYSTEM] Artifact Extraction Target: {self.artifact_dir}")
        
        try:
            # 1. Attach Auditors
            self.determinism_auditor.attach()
            
            # 2. Build Context (Scene으로 제어권 위임)
            context = ActContext(
                boundary=self.boundary,
                adapter=self.adapter,
                auditors={"determinism": self.determinism_auditor}
            )
            
            # 3. Execute Logical Suites
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    total_fails = await self._run_all_suites(broker, context)
                    if total_fails > 0:
                        return False, f"ACT runtime verified, but {total_fails} logical E2E tests failed."
            else:
                log.warning("[WARNING] No test suites provided. Exiting cleanly.")
            
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Orchestration process crashed: {e}")
            return False, str(e)
            
        finally:
            log.info("\n[SYSTEM] Initiating Teardown Sequence...")
            self.determinism_auditor.detach()
            
            # 워크스페이스(아티팩트 임시 폴더) 정리 로직
            if not self.keep_workspace:
                if self.artifact_dir.exists():
                    shutil.rmtree(self.artifact_dir, ignore_errors=True)
                    log.info(f"  └─ Cleaned up temporary artifact directory: {self.artifact_dir}")
            else:
                log.info(f"  └─ Artifacts preserved at: {self.artifact_dir}")
            
            log.info("[SYSTEM] CI Orchestration finalized.")

if __name__ == "__main__":
    async def main():
        orchestrator = ActOrchestrator()
        success, msg = await orchestrator.execute()
        if not success:
            log.critical(f"Pipeline Failed: {msg}")
    asyncio.run(main())