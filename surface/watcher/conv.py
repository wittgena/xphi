# surface.watcher.conv
import asyncio
import time
from typing import Dict, Any, List, Optional
from bridge.interface.pir import PsiEvent, PsiCarrier
from bridge.interface.topos import IPhaseAtor, IPhaseField, IEventBus
from bridge.interface.bus import AsyncEventBus

class SystemConv:
    """수치형 상태를 Anchor 방향으로 일정 비율(rate)만큼 당기는 범용 커널"""
    def __init__(self, convergence_rate: float = 0.5):
        self.rate = convergence_rate

    def compute_step(self, gradient: Dict[str, float], dt: float) -> Dict[str, float]:
        return {key: tension * self.rate * dt for key, tension in gradient.items()}

class ConvPhaseField(IPhaseField):
    """어떤 형태의 딕셔너리든 Anchor와 State를 관리하는 범용 필드"""
    def __init__(self, initial_state: Dict[str, float], initial_anchor: Dict[str, float], kernel: SystemConv):
        self.kernel = kernel
        self.state = initial_state.copy()
        self.anchor = initial_anchor.copy()
        self._prev_state = initial_state.copy()

        self._node_states = {}
        self._tensions = {}

    def get_state(self) -> Dict[str, Any]:
        return {"state": self.state, "anchor": self.anchor}

    def compute_gradient(self) -> Dict[str, float]:
        return {}

    def update_node_state(self, node_id: str, new_state: str) -> None:
        self._node_states[node_id] = new_state

    def set_tension(self, node_id: str, tension: float) -> None:
        self._tensions[node_id] = tension

    def evolve(self, gradient: Dict[str, float], dt: float):
        if not gradient: return
        
        self._prev_state = self.state.copy()
        deltas = self.kernel.compute_step(gradient, dt)
        
        for key, delta in deltas.items():
            if key in self.state:
                self.state[key] += delta

class GenericDriftEvaluator:
    """State와 Anchor 간의 교집합 키를 동적으로 순회하여 편차(Drift)를 계산"""
    def evaluate(self, field: ConvPhaseField) -> Dict[str, Any]:
        gradient = {}
        severity = 0.0
        
        common_keys = set(field.state.keys()) & set(field.anchor.keys())
        for key in common_keys:
            drift = field.anchor[key] - field.state[key]
            velocity = field.state[key] - field._prev_state.get(key, field.state[key])
            gradient[key] = drift
            severity += abs(drift) + abs(velocity)
            
        return {"gradient": gradient, "severity": severity}

class ThresholdBoundaryObserver:
    """임계치(tolerance)를 넘는 장력(Tension)만 필터링하는 범용 옵저버"""
    def __init__(self, tolerance: float = 0.5):
        self.tolerance = tolerance

    def extract(self, eval_result: Dict[str, Any]) -> Dict[str, float]:
        return {k: v for k, v in eval_result["gradient"].items() if abs(v) > self.tolerance}

class AnchorUpdateAtor(IPhaseAtor):
    """특정 이벤트를 감지하면 필드의 Anchor를 덮어쓰는 범용 액터"""
    def __init__(self, ator_id: str, trigger_kind: str):
        self._id = ator_id
        self.trigger_kind = trigger_kind
        self._state = "NORMAL"  # [Fix] 프로퍼티 에러 방지를 위한 초기화

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def react(self, event: PsiEvent, field: ConvPhaseField, bus: AsyncEventBus):
        if event.carrier.kind == self.trigger_kind:
            if "anchor" in event.carrier.payload:
                field.anchor.update(event.carrier.payload["anchor"])
                print(f"\n[Ator:{self._id}] 외부 이벤트 감지 -> Anchor 이동: {field.anchor}")

class StateUpdateIngestor:
    """지정된 이벤트의 Payload를 필드의 State에 직접 반영하는 범용 Ingestor"""
    def __init__(self, trigger_kind: str):
        self.trigger_kind = trigger_kind

    def ingest(self, field: ConvPhaseField, psi: PsiEvent):
        if psi.carrier.kind == self.trigger_kind:
            tag = psi.carrier.tag
            if "value" in psi.carrier.payload:
                field.state[tag] = psi.carrier.payload["value"]

class ConvSystem:
    """파이프라인 실행을 캡슐화한 런타임 시스템"""
    def __init__(self, field, evaluator, boundary, ingestors, bus):
        self.field = field
        self.evaluator = evaluator
        self.boundary = boundary
        self.ingestors = ingestors
        self.bus = bus

    def process_step(self, dt: float, injected_events: List[PsiEvent]) -> Dict[str, float]:
        """단일 Tick의 파이프라인(Ingest -> Evaluate -> Extract -> Evolve) 처리"""
        # 1. Ingest (센서 데이터 등 주입)
        for event in injected_events:
            for ingestor in self.ingestors:
                ingestor.ingest(self.field, event)

        # 2. Evaluate & Boundary Extract (편차 및 장력 계산)
        eval_result = self.evaluator.evaluate(self.field)
        gradient = self.boundary.extract(eval_result)

        # 3. Evolve (상태 진화)
        if gradient:
            self.field.evolve(gradient, dt)
            
        return gradient

class ConvSystemBuilder:
    """Configuration Dictionary 기반으로 시스템을 조립하는 빌더"""
    @staticmethod
    def build_from_dict(config: Dict[str, Any]) -> ConvSystem:
        kernel = SystemConv(convergence_rate=config.get("convergence_rate", 0.4))
        field = ConvPhaseField(
            initial_state=config["initial_state"],
            initial_anchor=config["initial_anchor"],
            kernel=kernel
        )
        evaluator = GenericDriftEvaluator()
        boundary = ThresholdBoundaryObserver(tolerance=config.get("tolerance", 0.5))

        bus = AsyncEventBus()
        bus.bind_field(field)

        ingestors = [StateUpdateIngestor(tk) for tk in config.get("ingest_triggers", [])]

        for ator_conf in config.get("ators", []):
            ator = AnchorUpdateAtor(ator_id=ator_conf["id"], trigger_kind=ator_conf["trigger"])
            bus.subscribe(ator)

        return ConvSystem(field, evaluator, boundary, ingestors, bus)

async def main():
    print("## [System Start] K8s GitOps 시나리오 (Data-Driven Builder 적용)\n")

    ## 1. 도메인 설정 데이터 (코드 수정 없이 JSON 파일 등으로 분리 가능)
    config = {
        "convergence_rate": 0.4,
        "tolerance": 0.5,
        "initial_state": {"cpu": 50.0, "replicas": 2.0},
        "initial_anchor": {"cpu": 50.0, "replicas": 2.0},
        "ingest_triggers": ["METRIC"],
        "ators": [
            {"id": "git.attractor", "trigger": "GIT_COMMIT_DONE"}
        ]
    }

    ## 2. 시스템 조립
    system = ConvSystemBuilder.build_from_dict(config)

    ## 3. 비동기 이벤트 시뮬레이션 (Git Commit)
    async def simulate_git():
        await asyncio.sleep(2.0)
        await system.bus.publish(PsiEvent(
            event_id="git-1", parent_id=None, source_id="github", scope="EXTERNAL", tick=int(time.time()),
            carrier=PsiCarrier(
                kind="GIT_COMMIT_DONE", tag="anchor", 
                payload={"anchor": {"cpu": 80.0, "replicas": 5.0}}
            )
        ))

    asyncio.create_task(simulate_git())

    ## 4. 물리적 환경 진화 루프
    dt = 1.0
    actual_k8s_cpu = 50.0

    for tick in range(15):
        ## 센서 관측 모사 (외부에서 들어온 이벤트 배열)
        events_this_tick = [
            PsiEvent(
                event_id=f"tick-{tick}", parent_id=None, source_id="prometheus", scope="LOCAL", tick=int(time.time()),
                carrier=PsiCarrier(kind="METRIC", tag="cpu", payload={"value": actual_k8s_cpu})
            )
        ]

        ## 캡슐화된 파이프라인 단일 스텝 실행
        gradient = system.process_step(dt=dt, injected_events=events_this_tick)
        
        ## 액추에이터가 물리적 상태를 변경했다고 가정 (피드백 루프)
        actual_k8s_cpu = system.field.state["cpu"] 

        ## 상태 로깅
        status_msg = f"Tick {tick:02d} | "
        if gradient:
            status_msg += f"∇Φ(장력)={gradient.get('cpu', 0):>6.2f} | 진화중 -> "
        else:
            status_msg += f"   동기화(안정상태) | 유지중 -> "
            
        status_msg += f"State: CPU {system.field.state['cpu']:.1f}, Reps {system.field.state.get('replicas', 0):.1f} "
        status_msg += f"(Anchor: CPU {system.field.anchor['cpu']:.1f})"
        print(status_msg)

        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())