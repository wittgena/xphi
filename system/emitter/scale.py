# system.emitter.scale
import asyncio
from kubernetes_asyncio import client, config
from bridge.psi import PsiEvent
from bound.interface import IPhaseAtor, IPhaseField
from bridge.bus import AsyncEventBus
from bound.emitter import get_logger
from contract.registry import ator_contract

log = get_logger("scale.emitter")

@ator_contract("scale.emitter")
class ScaleEmitter(IPhaseAtor):
    """
    @role: Φ(t) Projector
    @desc: Systemic PsiEvent를 수신하여 쿠버네티스 밀도(Scale)를 실시간 조율하는 에이전트
    """
    def __init__(self, ator_id: str = "runtime.morpher", namespace: str = "default", **kwargs):
        self._id = ator_id
        self._state = "IDLE"
        self.namespace = namespace
        self.k8s_apps = None
        self._initialized = False

    @property
    def ator_id(self) -> str: 
        return self._id

    @property
    def state(self) -> str: 
        return self._state

    def set_state(self, new_state: str) -> None: 
        self._state = new_state

    async def _initialize_k8s(self):
        """이벤트 루프 내에서 K8s 비동기 클라이언트를 지연 초기화 (Lazy Init)"""
        if self._initialized: return
        try:
            await config.load_kube_config()
        except:
            await config.load_incluster_config()
        self.k8s_apps = client.AppsV1Api()
        self._initialized = True
        log.info(f"[Φ(t)] Kubernetes Async Client initialized for Projector ({self._id}).")

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        # 1. 라우팅: 자신에게 할당된 제어 시그널(Ψ')인지 확인
        if event.carrier.kind != "AWS_SCALE_REQUEST":
            return

        carrier = event.carrier
        
        # 2. 위상 해석: Carrier에서 target(tag)과 phase(payload) 추출
        target_resource = carrier.tag
        target_phase = carrier.payload

        if not target_resource or not isinstance(target_phase, str):
            log.error(f"[Actuation Error] Invalid Control Signal (Ψ'): Missing target or phase in {carrier}")
            return

        log.info(f"[Φ(t) Modulation] Signal {event.event_id} routing {target_resource} to Phase {target_phase}")

        # 3. K8s 클라이언트 비동기 접속
        await self._initialize_k8s()

        # 4. 위상 전이 집행 매핑
        replicas = None
        if target_phase == "Φ0":
            replicas = 3
        elif target_phase == "∂Φ":
            replicas = 1  # 잉여 수축
        elif target_phase == "Φ4":
            replicas = 0  # 감각/방어 수축
        else:
            log.warning(f"Unknown phase request: {target_phase}")
            return

        # 5. 비동기 스케일 아웃/인 집행
        await self._project_scale(target_resource, replicas)
        self.set_state(f"PROJECTED_{target_phase}")

    async def _project_scale(self, name: str, replicas: int):
        """
        [합성 규칙] Φ(t)는 구조(spec.template)를 건드리지 않고,
        오직 밀도(spec.replicas)만 Scale Subresource를 통해 패치합니다.
        """
        body = {'spec': {'replicas': replicas}}
        try:
            await self.k8s_apps.patch_namespaced_deployment_scale(
                name=name, namespace=self.namespace, body=body
            )
            log.info(f"  -> Modulated density of '{name}' to {replicas}")
        except client.exceptions.ApiException as e:
            if e.status != 404:
                log.error(f"  -> Failed to modulate {name}: {e}")
            else:
                log.warning(f"  -> Target '{name}' not found in namespace '{self.namespace}'.")