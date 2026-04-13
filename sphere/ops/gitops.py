# sphere.ops.gitops
import asyncio
import yaml # 실제 운영 환경에서는 주석을 보존하는 ruamel.yaml 사용을 권장합니다.
from pathlib import Path
from bridge.pir import PsiEvent
from topos.bound import IPhaseAtor, IPhaseField
from bridge.bus import AsyncEventBus
from plane.emitter import get_logger
from contract.registry import ator_contract

log = get_logger("gitops.emitter")

@ator_contract("gitops.emitter")
class GitOpsEmitter(IPhaseAtor):
    """
    @role: Φ(t) GitOps Projector
    @desc: 제어 시그널을 수신하여 Git Repository의 매니페스트(YAML)를 수정하고 Commit/Push 하는 에이전트
    """
    def __init__(self, ator_id: str, repo_path: str, manifest_file: str, branch: str = "main", **kwargs):
        self._id = ator_id
        self._state = "IDLE"
        
        self.repo_path = Path(repo_path)
        self.manifest_file = self.repo_path / manifest_file
        self.branch = branch

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def _run_git(self, *args) -> str:
        """비동기 Git 명령어 실행기"""
        proc = await asyncio.create_subprocess_exec(
            'git', *args,
            cwd=self.repo_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise Exception(f"Git command failed: {stderr.decode().strip()}")
        return stdout.decode().strip()

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        if event.carrier.kind != "AWS_SCALE_REQUEST":
            return

        carrier = event.carrier
        target_resource = carrier.tag
        target_phase = carrier.payload

        if not target_resource or not isinstance(target_phase, str):
            return

        log.info(f"[GitOps Projection] Signal {event.event_id} routing {target_resource} to Phase {target_phase}")

        # 위상 매핑
        replicas = 3 if target_phase == "Φ0" else (1 if target_phase == "∂Φ" else 0)

        try:
            # 1. 최신 상태 동기화 (Pull)
            await self._run_git("checkout", self.branch)
            await self._run_git("pull", "origin", self.branch)

            # 2. YAML 파일 파싱 및 밀도(Scale) 조작
            modified = self._patch_yaml_replicas(replicas)
            if not modified:
                log.info(f"  -> No changes needed for {target_resource}. Replicas already at {replicas}.")
                return

            # 3. 변경사항 Commit & Push
            commit_msg = f"[Systemic Watcher] Phase Transition {target_phase}: scale {target_resource} to {replicas}"
            await self._run_git("add", str(self.manifest_file))
            await self._run_git("commit", "-m", commit_msg)
            await self._run_git("push", "origin", self.branch)
            
            self.set_state(f"COMMITTED_{target_phase}")
            log.info(f"  -> Successfully pushed Phase {target_phase} to Git Repository.")

        except Exception as e:
            log.error(f"[GitOps Error] Failed to project phase: {e}")

    def _patch_yaml_replicas(self, target_replicas: int) -> bool:
        """YAML 파일을 읽어 replicas 값을 수정. (변경이 발생했는지 boolean 반환)"""
        if not self.manifest_file.exists():
            raise FileNotFoundError(f"Manifest not found: {self.manifest_file}")

        with open(self.manifest_file, 'r') as f:
            docs = list(yaml.safe_load_all(f))

        changed = False
        for doc in docs:
            # 타겟 리소스(Deployment)를 찾아 replicas 필드 갱신
            if doc and doc.get("kind") == "Deployment":
                spec = doc.get("spec", {})
                current_replicas = spec.get("replicas")
                if current_replicas != target_replicas:
                    spec["replicas"] = target_replicas
                    changed = True

        if changed:
            with open(self.manifest_file, 'w') as f:
                yaml.dump_all(docs, f, default_flow_style=False)
                
        return changed