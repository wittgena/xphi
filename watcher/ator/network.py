# watcher.ator.network
## @lineage: surface.ator.network
## @lineage: xyz.surface.ator.network
## @lineage: xyz.subst.ator.network
## @lineage: foldbox.manager.workspace.network
## @lineage: xyz.workspace.network
## @lineage: xyz.field.network
## @lineage: phase.watcher.field.network
## @lineage: meta.watcher.field.network
## @lineage: topos.watcher.field.network
## @lineage: cognitive.watcher.field.network
## @lineage: cognitive.field.network
## @lineage: cognitive.node.network
import math
import random
from typing import Dict, Any, List, Optional
from arch.contract.registry.unified import contract
from arch.contract.interface import IPhaseField

@contract.field("node.network")
class NodeNetwork(IPhaseField):
    """
    @role: 위상 동기화가 일어나는 캔버스이자 XeCont가 래핑하는 물리적 Bound 객체
    @flow: Ψ(이벤트) 흡수 -> Kernel을 통한 위상 진화 -> Watcher 감시 -> 파열 시 Regime 적용
    """
    def __init__(self, **kwargs):
        self.size = kwargs.get("size", 10)
        self.init_phase_range = kwargs.get("init_phase_range", [0.0, 1.0])
        self.omega_range = kwargs.get("omega_range", [0.8, 1.2])

        # 의존성 주입 공간
        self.kernel = None
        self.watcher = None
        self.regime = None
        self.ators = []

        # Bound 물리량 (XeCont가 접근함)
        self._states: Dict[str, Dict[str, Any]] = {}
        self.pressure: float = 0.0  # 현재 장의 총 누적 피로도 (Tension)
        self.topology: int = 1      # 현재 장의 위상 에포크 (Epoch)

    # --- SystemBuilder Binding Methods ---
    def bind_kernel(self, kernel): self.kernel = kernel
    def bind_watcher(self, watcher): self.watcher = watcher
    def bind_regime(self, regime): self.regime = regime
    def bind_ators(self, ators):
        self.ators = ators
        for a in ators:
            self._states[a.ator_id] = {
                "phase": random.uniform(*self.init_phase_range) * math.pi * 2,
                "omega": random.uniform(*self.omega_range),
                "state": getattr(a, "initial_state", "NORMAL"),
                "tension": 0.0
            }

    # --- IPhaseField Interface ---
    def get_state(self) -> Dict[str, Any]:
        return self._states

    def compute_gradient(self) -> Dict[str, float]:
        return {node_id: data["tension"] for node_id, data in self._states.items()}

    def evolve(self, dt: float) -> None:
        if not self.kernel: return
        
        # Kernel(예: KuramotoSensor)에 상태를 넘겨 미분값(Delta) 계산
        deltas = self.kernel.compute_step(self._states, dt)
        
        total_tension = 0.0
        for node_id, delta in deltas.items():
            # 1. 노드별 위상 진화
            self._states[node_id]["phase"] = (self._states[node_id]["phase"] + delta["d_phase"]) % (2 * math.pi)
            
            # 2. 핵심 해결책: 텐션(피로도)의 '누적' (+=)
            # 과거의 덮어쓰기(=)를 누적으로 변경하여 스트레스가 쌓이도록 합니다.
            self._states[node_id]["tension"] += (delta["target_tension"] * dt)
            
            total_tension += self._states[node_id]["tension"]
            
        # Field의 전체 압력(Pressure) 갱신
        self.pressure = total_tension / max(1, len(self._states))

        # 3. 관측(Observability) 확보: 콘솔에 위상 장의 현재 상태 렌더링
        if hasattr(self.kernel, 'render_state'):
            visual = self.kernel.render_state(self._states)
            # \r을 사용하여 한 줄에서 애니메이션처럼 보이게 출력
            print(f"\r[Phase Field] {visual} | Pressure: {self.pressure:.2f}/17.0 ", end="", flush=True)

    def absorb(self, batch_payload: List[Dict[str, Any]]):
        """XeCont.execute Step 1: 흡수 및 진화 (dt=0.1 고정으로 가정)"""
        ## 외부 payload(이벤트)가 있다면 일부 노드 상태에 개입
        self.evolve(dt=0.1)

    def evaluate(self) -> str:
        """XeCont.execute Step 2: 임계점 평가"""
        if self.watcher:
            ## Watcher가 파열을 감지하면 DEPOSIT(단절)을 반환
            trigger = self.watcher.evaluate(self, history=[], current_tick=0)
            if trigger and getattr(trigger.carrier, 'kind', '') == "RUPTURE":
                return "DEPOSIT"
        return "SATURATE"

    def commit(self):
        """XeCont.execute Step 2-1: 파열 확정 시 상태 커밋 및 전이"""
        if self.regime:
            self.regime.modify_field(self)
        # 상전이 완료: 위상 에포크 증가
        self.topology += 1