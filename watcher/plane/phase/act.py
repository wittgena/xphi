# xphi.watcher.plane.phase.act
import os
import sys
import json
import shutil
import platform
import asyncio
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Tuple

from xphi.arch.dev.tracer.base import SystemBound
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.kernel.space.bind.resolver import resolve_path

TIME_ROOT = resolve_path("time")
log = get_emitter("plane.phase.act")

class ActBlueprint:
    @staticmethod
    def get_ci_spec() -> Dict[str, Dict[str, Any]]:
        """Defines the environment variables and descriptions for each CI pipeline job."""
        return {
            "system-e2e-test": {
                "env": {}, 
                "description": "System E2E validation (Infrastructure & Intent)"
            },
            "build-release": {
                "env": {"FIBER_BUILD_DIST": "1"}, 
                "description": "Deterministic remote binding for Wheel artifact"
            }
        }

class BaseActAdapter:
    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        raise NotImplementedError

class NektosActAdapter(BaseActAdapter):
    def __init__(self, workspace: Path, boundary: SystemBound, artifact_dir: Path, rebuild: bool = False):
        self.workspace = workspace
        self.boundary = boundary
        self.artifact_dir = artifact_dir
        self.image_name = "fiber-act-runner:latest"
        self.rebuild = rebuild
        
        if not shutil.which("act"):
            raise RuntimeError("[CRITICAL] 'act' binary not found. Standard CI emulation unavailable.")

    async def _prepare_golden_image(self) -> str:
        """Builds the optimized E2E image based on Dockerfile.xphi to bypass architectural emulation issues."""
        dockerfile_path = TIME_ROOT / "Dockerfile.xphi"
        
        if not dockerfile_path.exists():
            log.warning(f"  ├─ ⚠️ Dockerfile not found at {dockerfile_path}. Falling back to default act image.")
            return "catthehacker/ubuntu:act-latest"
            
        log.info(f"  ├─ Building Golden Base Image ({self.image_name}) from {dockerfile_path}...")
        
        cmd = [
            "docker", "build", 
            "-t", self.image_name, 
            "-f", str(dockerfile_path), 
            str(self.workspace)
        ]
        
        code, _, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=True)
        if code != 0:
            raise RuntimeError(f"Failed to build custom act image: {err}")
            
        log.info("  ├─ Golden Image ready ✅")
        return self.image_name

    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        log.info(f"[Adapter:ACT] Provisioning Runner for Job: {job_name}")
        
        target_image = await self._prepare_golden_image()
        
        # Inject mock GitHub context to bypass YAML branch/label constraints locally
        mock_payload_path = self.artifact_dir / f"mock_payload_{job_name}.json"
        mock_payload = {
            "ref": "refs/tags/v1.0.1+bound",
            "pull_request": {
                "labels": [{"name": "e2e-approved"}]
            }
        }
        
        with open(mock_payload_path, "w") as f:
            json.dump(mock_payload, f)
            
        log.info(f"  ├─ Mock Webhook Payload injected: {mock_payload['ref']}")

        cmd = [
            "act", "push", "-j", job_name, 
            "--artifact-server-path", str(self.artifact_dir),
            "-P", f"ubuntu-latest={target_image}",
            "-e", str(mock_payload_path),
            "--pull=false",
            # Enforce strict OOM killer by disabling swap memory (resolves virtualization I/O thrashing)
            "--container-options", "--memory=2g --memory-swap=2g"
        ]
        
        if self.rebuild:
            cmd.append("--rebuild")
            log.info("  ├─ Rebuild flag injected: Forcing clean cache.")
        
        gh_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if gh_token:
            cmd.extend(["-s", f"GITHUB_TOKEN={gh_token}"])
            log.info("  ├─ GITHUB_TOKEN secret securely injected.")
        else:
            log.warning("  ├─ ⚠️ GITHUB_TOKEN not found in env. Clone operations might fail due to rate limits.")

        for k, v in env.items():
            cmd.extend(["--env", f"{k}={v}"])
            log.info(f"  ├─ Injecting Env: {k}={v}")

        code, _, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        
        if code != 0:
            log.error(f"[Adapter:ACT] Job '{job_name}' execution fractured.")
            return False
            
        return True

class DeterminismAuditor:
    """Audits the dependency integrity of the built artifacts to prevent local path leakage."""
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
        
        wheel_files = list(self.target_dir.rglob("fiber-*.whl"))
        
        if not wheel_files:
            log.error("  └─ 💥 Artifact missing. Build phase did not produce or upload a Wheel.")
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
    adapter: BaseActAdapter
    auditors: Dict[str, Any]

class ActOrchestrator:
    def __init__(self, mode: str = "dev", suites: Dict[str, Any] = None, rebuild: bool = False):
        self.mode = mode
        self.suites = suites or {}
        self.keep_workspace = False
        
        self.workspace = Path.cwd()
        self.boundary = SystemBound()
        
        self.artifact_dir = Path(tempfile.mkdtemp(prefix="fiber-act-artifacts-"))
        
        self.adapter = NektosActAdapter(self.workspace, self.boundary, self.artifact_dir, rebuild)
        self.determinism_auditor = DeterminismAuditor(self.artifact_dir, self.boundary)
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
            self.determinism_auditor.attach()
            context = ActContext(
                boundary=self.boundary,
                adapter=self.adapter,
                auditors={"determinism": self.determinism_auditor}
            )
            
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