# xphi.watcher.plane.infra.kube
"""
@desc: 
- Kubernetes (Minikube) Native Infrastructure Orchestrator.
- Manages dynamic topology provisioning, port-forwarding, and shell execution.
- Delegates YAML rendering to xphi.watcher.plane.infra.yaml.deployment.
"""
import os
import shutil
import socket
import asyncio
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional, Type

from xphi.arch.dev.tracer.base import SystemBound
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.plane.infra.yaml.deployment import KubeBlueprint

log = get_emitter("infra.kube")

class MinikubeNativeAdapter:
    def __init__(self, workspace: Path, boundary: SystemBound, artifact_dir: Path, 
                 namespace: str = "fiber-topos", rebuild: bool = False, base_env: Optional[Dict[str, str]] = None):
        self.workspace = workspace
        self.boundary = boundary
        self.artifact_dir = artifact_dir
        self.namespace = namespace
        self.rebuild = rebuild
        self.manifest_file = self.workspace / "k8s-gateway.yaml"
        
        # 포트 포워딩 프로세스 관리용
        self._port_forward_processes: Dict[int, asyncio.subprocess.Process] = {}
        
        # 글로벌 환경변수 초기화
        self.global_env = {
            "XPHI_ENV": "ci",
            "VCR_MODE": "live",
            "NODE_PROFILE": "EDGE"
        }
        if base_env:
            self.global_env.update(base_env)

        # 자체 Redis 터널 배포 여부 판단 (외부 URL이 제공되지 않은 경우 배포)
        self.deploy_internal_redis = "REDIS_URL" not in self.global_env
        if self.deploy_internal_redis:
            self.global_env["REDIS_URL"] = "redis://fiber-tunnel:6379/0"

    async def _build_local_node_image(self) -> bool:
        """Minikube의 내장 데몬을 사용하여 로컬 이미지를 직접 빌드합니다."""
        log.info("  ├─ [Minikube] Building 'fiber-node:local' inside cluster...")
        cmd = ["minikube", "image", "build", "-t", "fiber-node:local", "-f", "Dockerfile.xphi", "."]
        
        if self.rebuild:
            cmd.append("--no-cache")
            log.info("  ├─ [Force Rebuild] Using --no-cache.")
            
        code, _, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        return code == 0

    async def _get_gateway_pod_name(self) -> str:
        """현재 네임스페이스에 배포된 Gateway Pod의 실제 이름을 조회합니다."""
        cmd = f"kubectl get pods -n {self.namespace} -l app=fiber-gateway -o jsonpath='{{.items[0].metadata.name}}'"
        code, out, _ = await self.boundary.run_command(["bash", "-c", cmd], capture=True)
        return out.strip() if code == 0 else ""

    async def apply(self) -> bool:
        """Kubernetes 인프라 프로비저닝 메인 플로우"""
        self.workspace.mkdir(parents=True, exist_ok=True)
        
        # 1. 이미지 빌드
        if not await self._build_local_node_image():
            log.error("  └─ 💥 Minikube image build failed.")
            return False

        # 2. YAML 매니페스트 렌더링 (분리된 모듈 활용)
        blueprint_yaml = KubeBlueprint.generate_manifest(
            namespace=self.namespace,
            global_env=self.global_env,
            deploy_redis=self.deploy_internal_redis
        )
        self.manifest_file.write_text(blueprint_yaml, encoding="utf-8")
        
        if self.deploy_internal_redis:
            log.info("  ├─ [K8s] Provisioning Internal Redis Tunnel...")
        else:
            log.info(f"  ├─ [K8s] Binding to External Redis: {self.global_env['REDIS_URL']}")

        log.info(f"  ├─ [K8s] Applying Native Topology to namespace '{self.namespace}'...")
        await self.boundary.run_command(["kubectl", "create", "namespace", self.namespace], capture=True)
        
        apply_cmd = ["kubectl", "apply", "-f", str(self.manifest_file), "-n", self.namespace]
        code, _, err = await self.boundary.run_command(apply_cmd, capture=True)
        if code != 0:
            log.error(f"  └─ 💥 Kubectl apply failed: {err}")
            return False

        # 3. Pod 가용성 대기 (Readiness Probe 확인)
        log.info("  ├─ [K8s] Awaiting Pod Readiness...")
        wait_cmd = ["kubectl", "wait", "--for=condition=Ready", "pod", "--all", "-n", self.namespace, "--timeout=120s"]
        code, _, _ = await self.boundary.run_command(wait_cmd, capture=False)
        
        if code != 0:
            log.error("  └─ 💥 Pods failed to reach Ready state.")
            return False
            
        log.info("  ├─ Runtime Topology (KUBE Mode) Ready ✅")
        return True

    # -------------------------------------------------------------------------
    # Network Tunneling (Port Forwarding)
    # -------------------------------------------------------------------------
    async def start_port_forward(self, local_port: int = 8000, target_port: int = 8000, service_name: str = "fiber-gateway") -> bool:
        """SDK 테스트를 위해 K8s Service 포트를 로컬 호스트로 개방합니다."""
        if local_port in self._port_forward_processes:
            log.warning(f"  ├─ [PortForward] Local port {local_port} is already being forwarded.")
            return True

        log.info(f"  ├─ [PortForward] Tunneling: 127.0.0.1:{local_port} -> svc/{service_name}:{target_port}")
        cmd = ["kubectl", "port-forward", f"svc/{service_name}", f"{local_port}:{target_port}", "-n", self.namespace]
        
        try:
            # 백그라운드 프로세스로 실행 유지
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=str(self.workspace)
            )
            
            # 소켓 연결이 성공할 때까지 최대 3초 대기
            is_open = False
            for _ in range(30):
                await asyncio.sleep(0.1)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    if s.connect_ex(('127.0.0.1', local_port)) == 0:
                        is_open = True
                        break
                        
            if not is_open:
                log.error(f"  └─ 💥 [PortForward] Failed to bind local port {local_port} within timeout.")
                process.terminate()
                return False
                
            self._port_forward_processes[local_port] = process
            log.info("  └─ ✨ [PortForward] Tunnel established successfully.")
            return True
            
        except Exception as e:
            log.error(f"  └─ 💥 [PortForward] Failed: {e}")
            return False

    async def stop_port_forward(self, local_port: int = None):
        """활성화된 포트 포워딩 프로세스를 안전하게 닫습니다."""
        ports_to_close = [local_port] if local_port else list(self._port_forward_processes.keys())
        for p in ports_to_close:
            process = self._port_forward_processes.pop(p, None)
            if process:
                log.info(f"  ├─ [PortForward] Closing tunnel on port {p}...")
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    log.warning(f"  ├─ [PortForward] Process on port {p} did not exit cleanly, killing it.")
                    process.kill()

    # -------------------------------------------------------------------------
    # Execution & Extractor
    # -------------------------------------------------------------------------
    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        """kubectl exec를 활용하여 파드 내부에 명령어와 동적 환경변수를 주입합니다."""
        log.info(f"[Adapter:KUBE] Executing Job Phase: {job_name}")
        
        pod_name = await self._get_gateway_pod_name()
        if not pod_name:
            log.error("  └─ 💥 Gateway Pod not found.")
            return False

        # 의도에 따른 명령어 파싱
        if job_name == "system-e2e-test" or "FIBER_E2E_STEPS" in env:
            exec_command = env.get("FIBER_E2E_STEPS", "fiber e2e dphi.wasm.entry && VCR_MODE=replay fiber ex switch")
        elif job_name == "build-release":
            exec_command = "python -m build --wheel && cp dist/*.whl /artifact_mount/"
        else:
            log.error(f"  └─ Unknown job phase: {job_name}")
            return False

        # 런타임 쉘 변수 주입용 export 체인 생성
        env_exports = " ".join([f"export {k}='{v}';" for k, v in env.items()])
        full_command = f"{env_exports} {exec_command}"
        
        for k, v in env.items():
            log.info(f"  ├─ Injecting Env Override: {k}={v}")

        cmd = ["kubectl", "exec", "-i", pod_name, "-n", self.namespace, "--", "bash", "-c", full_command]
        code, _, _ = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        
        if code != 0:
            log.error(f"[Adapter:KUBE] Phase '{job_name}' fractured.")
            return False

        # 빌드 아티팩트 추출 (kubectl cp)
        if job_name == "build-release":
            log.info("  ├─ Extracting built artifacts from K8s pod volume...")
            cp_cmd = ["kubectl", "cp", f"{self.namespace}/{pod_name}:/artifact_mount/.", str(self.artifact_dir)]
            cp_code, _, _ = await self.boundary.run_command(cp_cmd, capture=True)
            if cp_code != 0:
                 log.error("  └─ 💥 Failed to extract artifacts via kubectl cp.")
                 return False

        log.info(f"  └─ Phase '{job_name}' completed successfully.")
        return True

    async def teardown(self):
        """사용된 인프라 리소스와 프로세스를 정리합니다."""
        await self.stop_port_forward()
        log.info(f"  ├─ [K8s] Tearing down Kube topology (Namespace: {self.namespace})...")
        cmd = ["kubectl", "delete", "namespace", self.namespace, "--ignore-not-found=true"]
        await self.boundary.run_command(cmd, capture=False)


class DeterminismAuditor:
    """Audits built artifacts for dependency integrity to prevent local path leakage."""
    def __init__(self, target_dir: Path, boundary: SystemBound):
        self.target_dir = target_dir
        self.boundary = boundary
        self.is_clean = False

    def attach(self): pass 
    def detach(self): pass

    async def verify(self) -> bool:
        log.info(f"[Auditor:Determinism] Inspecting extracted Artifacts in {self.target_dir}...")
        wheel_files = list(self.target_dir.rglob("fiber-*.whl"))
        
        if not wheel_files:
            log.error("  └─ 💥 Artifact missing. Wheel not found in the extracted directory.")
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
class KubeContext:
    boundary: SystemBound
    adapter: MinikubeNativeAdapter
    auditors: Dict[str, Any]


class KubeOrchestrator:
    def __init__(self, mode: str = "dev", suites: Dict[str, Type] = None, rebuild: bool = False, base_env: Dict[str, str] = None):
        self.mode = mode
        self.suites = suites or {}
        self.keep_workspace = False
        
        self.workspace = Path.cwd()
        self.boundary = SystemBound()
        
        self.artifact_dir = Path(tempfile.mkdtemp(prefix="fiber-kube-artifacts-"))
        os.environ["FIBER_ARTIFACT_MOUNT"] = str(self.artifact_dir)
        
        self.adapter = MinikubeNativeAdapter(
            workspace=self.workspace, 
            boundary=self.boundary, 
            artifact_dir=self.artifact_dir,
            rebuild=rebuild,
            base_env=base_env
        )
        self.determinism_auditor = DeterminismAuditor(self.artifact_dir, self.boundary)
        self.suite_runners = {}

    async def _run_all_suites(self, broker: Any, context: KubeContext) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Executing K8s CI Suite: {suite_name.upper()} <<<")
            try:
                # E2E Scene 인스턴스화 (e2e.plane.kube 등에서 전달된 Scene 클래스)
                suite_instance = suite_cls(broker=broker, context=context)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating CI/CD Plane via Kubernetes ({self.mode.upper()}) ---")
        log.info(f"[SYSTEM] Artifact Extraction Target: {self.artifact_dir}")
        
        try:
            self.determinism_auditor.attach()
            context = KubeContext(
                boundary=self.boundary,
                adapter=self.adapter,
                auditors={"determinism": self.determinism_auditor}
            )
            
            # 인프라 프로비저닝 (Manifest 적용)
            if not await self.adapter.apply():
                return False, "Failed to provision Kubernetes infrastructure."

            # E2E 테스트 스위트 구동
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    total_fails = await self._run_all_suites(broker, context)
                    if total_fails > 0:
                        return False, f"Topology verified, but {total_fails} logical E2E tests failed."
            
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Orchestration process crashed: {e}")
            return False, str(e)
            
        finally:
            log.info("\n[SYSTEM] Initiating Teardown Sequence...")
            self.determinism_auditor.detach()
            
            if not self.keep_workspace:
                await self.adapter.teardown()
                if self.artifact_dir.exists():
                    shutil.rmtree(self.artifact_dir, ignore_errors=True)
                    log.info(f"  └─ Cleaned up temporary artifact directory: {self.artifact_dir}")
            else:
                log.info(f"  └─ Workspace preserved. Inspect via: kubectl get pods -n {self.adapter.namespace}")
            
            log.info("[SYSTEM] Kube Orchestration finalized.")