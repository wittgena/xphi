# nexus.exp.bound.tracer
import time
from dataclasses import dataclass, field
from typing import List, Optional
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.tracer")

## 기저 어휘 및 엔티티 (Lexicon & Entities)
@dataclass
class Psi:
    """외부 자극 (External Signal / Traffic)"""
    name: str
    energy: float

@dataclass
class Residue:
    """위상 파열 시 발생하는 찌꺼기 (Dead Letter / Failed Payments)"""
    source_bound: str
    overflow_energy: float
    timestamp: float = field(default_factory=time.time)


## 메타 관측기 (Isorhesis Tracer)
class IsoTracer:
    """
    [동적 평형 관측기]
    경계가 파열될 위기에 처했을 때 시스템이 스스로 한계치를 늘려(Scale-out)
    어떻게 평형을 달성하는지 외부에서 관측하고 증명합니다.
    """
    def __init__(self):
        self.shift_history: List[tuple] = []
        self.rupture_history: List[Residue] = []

    def record_shift(self, old_limit: float, new_limit: float):
        log.info(f"  [Tracer: Isorhesis] Boundary Shift 관측: {old_limit:.1f} -> {new_limit:.1f} (경계 팽창)")
        self.shift_history.append((old_limit, new_limit))

    def record_rupture(self, residue: Residue):
        log.info(f"  [Tracer: Rupture] 평형 붕괴! 치명적 잔여물 발생: {residue.overflow_energy:.1f}")
        self.rupture_history.append(residue)


## 위상 경계 엔진 (Topological Bound)
class DynamicBound:
    """
    [유동적 위상 경계막]
    topic.builder의 '동적 경계 획정' 철학과 결합되어, 
    고정된 임계치가 아닌 시스템의 최대 탄성치(Elasticity) 내에서 한계를 유동적으로 제어합니다.
    """
    def __init__(self, name: str, base_threshold: float, max_elasticity: float, tracer: IsoTracer):
        self.name = name
        self.base_threshold = base_threshold       # 기본 평형 상태의 한계치 (ex: Pod 1대의 처리량)
        self.current_threshold = base_threshold    # 현재 팽창된 한계치
        self.max_threshold = max_elasticity        # 찢어지기 전까지 버틸 수 있는 최대 탄성 (ex: Max Replicas)
        self.current_tension = 0.0
        self.tracer = tracer

    def inject(self, psi: Psi) -> Optional[Residue]:
        log.info(f"\n[{self.name}] 텐션 유입(Ψ): '{psi.name}' (에너지: {psi.energy})")
        self.current_tension += psi.energy

        if self.current_tension <= self.current_threshold:
            return self._absorb()
        else:
            return self._evaluate_homeostasis()

    def _absorb(self) -> None:
        log.info(f" └─ 상태: [평형 유지] 자극 흡수 완료. (Tension: {self.current_tension:.1f} / {self.current_threshold:.1f})")
        return None

    def _evaluate_homeostasis(self) -> Optional[Residue]:
        """임계치 초과 시 즉시 파열하지 않고 Boundary Shift (동적 평형) 시도"""
        required_threshold = self.current_tension

        if required_threshold <= self.max_threshold:
            # Isorhesis (Scale-out) 발동: 경계를 팽창시켜 텐션을 수용
            old_threshold = self.current_threshold
            self.current_threshold = required_threshold
            
            log.info(f" └─ 상태: [동적 평형 시도] 임계치를 초과하여 경계막 팽창을 시작합니다...")
            self.tracer.record_shift(old_threshold, self.current_threshold)
            log.info(f" └─ 결과: [흡수 성공] 파열을 방어했습니다. (Tension: {self.current_tension:.1f} / {self.current_threshold:.1f})")
            return None
        else:
            # 최대 탄성치 초과 -> Rupture (위상 붕괴)
            return self._collapse()

    def _collapse(self) -> Residue:
        """절대 경계의 파열 및 잔여물 방출"""
        overflow = self.current_tension - self.current_threshold
        log.info(f" └─ 상태: [경계 파열 / Rupture!] 시스템의 최대 탄성({self.max_threshold:.1f})을 초과했습니다.")
        
        # 시스템 보호를 위해 장력을 현재 임계치로 리셋하고 초과분만 방출
        self.current_tension = self.current_threshold
        
        residue = Residue(source_bound=self.name, overflow_energy=overflow)
        self.tracer.record_rupture(residue)
        return residue


if __name__ == "__main__":
    # 관측기 부착
    tracer = IsoTracer()

    # 결제 도메인: 기본 처리량 10.0, 최대 확장(Scale-out) 처리량 25.0 인 강직한 경계
    payment_bound = DynamicBound(
        name="Payment_Core", 
        base_threshold=10.0, 
        max_elasticity=25.0, 
        tracer=tracer
    )

    ## 일상적인 트래픽 (기본 임계치 내)
    payment_bound.inject(Psi("오전 정기 결제", energy=4.0))
    payment_bound.inject(Psi("B2B API 호출", energy=5.0))

    ## 비대칭 스파이크 발생 (기본 임계치 초과 -> 경계 팽창 발동)
    payment_bound.inject(Psi("블랙 프라이데이 특가 이벤트 시작", energy=12.0))

    ## 추가 폭주 트래픽 발생 (최대 탄성치 초과 -> 파열 발생)
    residue = payment_bound.inject(Psi("봇 넷 트래픽 동시 유입", energy=15.0))

    ## 관측 종료 및 잔여물 처리 결과
    log.info("\n" + "="*50)
    log.info("## [Meta-Observation] 시스템 궤적 리포트")
    log.info(f" - Boundary Shifts (팽창 횟수): {len(tracer.shift_history)}")
    if tracer.rupture_history:
        log.info(f" - System Ruptured! 치명적 결제 유실({residue.overflow_energy:.1f}) 발생.")
        log.info(f" - Action: ResidueStore(DLQ)에 저장 후 Surgent 엔진으로 분석 회부 요망.")
    log.info("="*50)