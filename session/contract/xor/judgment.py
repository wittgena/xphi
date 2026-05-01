# session.contract.xor.judgment
import asyncio
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from session.bound.client.local.lm import LocalLM
from session.contract.xor.store import ResidueStore, ResidueSnapshot
from meta.flow.surface.emitter import get_emitter

log = get_emitter("proto.flow")

@dataclass
class Signal:
    """Ψ: 단순 텍스트가 아닌, 시스템 내부에서 전파되는 파동(Wave)"""
    source: str
    pressure: float
    frequency: str
    payload: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class Residue:
    """xe: 구조적 정합에 실패한 구체적 파편(Rupture)"""
    topos_path: str
    dissonance_type: str
    content: str

class JudgmentField:
    """Φ': 단발적 판단이 아닌, 신호들이 간섭하며 형성되는 연속적인 장"""
    def __init__(self):
        self.potential_energy = 0.0
        self.interference_pattern = []  # 누적된 판단의 궤적
        self.stable_projection = None

    def update_field(self, signal: Signal, xe_list: List[Residue]):
        """새로운 신호와 잔여물을 장에 중첩(Superposition)시킨다"""
        self.potential_energy += signal.pressure * (1.0 + len(xe_list))
        self.interference_pattern.append({
            "t": signal.timestamp,
            "energy": self.potential_energy,
            "ruptures": [r.dissonance_type for r in xe_list]
        })

class XorJudgment:
    def __init__(self):
        self.store = ResidueStore()
        self.lm = LocalLM()
        self.phi_prime = JudgmentField()  # 이제 Φ'은 객체로서 존재 (Continuous Field)
        self.memory_path = [] # 시간 축에 따른 xe의 누적 궤적

    async def execute(self, entry_point: str):
        log.signal("System Ignition: 2nd Folding - Introducing Time Axis.")
        
        current_input = entry_point
        cycle = 0

        while True:
            cycle += 1
            log.info(f"--- Cycle {cycle}: Attractor Seeking ---")

            # 1. Flow (Ψ): 텍스트를 고압력 신호로 변환
            signal = await self._psi_propagation(current_input, cycle)

            # 2. Judgment (Φ'): 장(Field)을 업데이트하고 결합 시도
            # 이제 '판단'은 함수 결과가 아니라 '장의 변화' 그 자체가 됩니다.
            xe_list, projection = await self._integrate_into_field(signal)
            self.phi_prime.update_field(signal, xe_list)

            # 3. Rhythm: 누적된 xe와 에너지 상태를 보고 재진입 결정
            should_continue, next_input = self._rhythm_reentry(xe_list, cycle)
            
            if not should_continue:
                break
            
            current_input = next_input

        return self.phi_prime.stable_projection

    async def _psi_propagation(self, raw_input: str, cycle: int) -> Signal:
        """단순 텍스트를 시스템 압력이 실린 Signal 객체로 승격"""
        log.info("Operator [Ψ]: Generating Signal Wave...")
        # LM을 통해 의미론적 텍스트를 구조적 긴장으로 번역
        resp = await self.lm.ask(f"Translate to structural signal: {raw_input}")
        
        return Signal(
            source="re-entry" if cycle > 1 else "origin",
            pressure=1.0 / cycle,
            frequency="high-interference" if "xe" in raw_input else "stable",
            payload=resp
        )

    async def _integrate_into_field(self, signal: Signal):
        """[Φ'] 신호를 기존 장에 투영하여 결합과 파편을 추출"""
        log.info(f"Field [Φ']: Testing Gluing Condition at energy {self.phi_prime.potential_energy}")
        
        # 여기서 LM은 '판단자'가 아니라 '물리 법칙의 시뮬레이터'로 작동합니다.
        resp = await self.lm.ask(f"Field State: {self.phi_prime.interference_pattern}\nNew Signal: {signal.payload}")
        
        # 파싱 로직은 이제 '객체'를 지향합니다.
        xe_list = [Residue(topos_path="root.node", dissonance_type="type_a", content="...")] 
        projection = {"state": "evolving", "energy": self.phi_prime.potential_energy}
        
        return xe_list, projection

    def _rhythm_reentry(self, xe_list: List[Residue], cycle: int) -> (bool, str):
        """시간 축을 고려한 안정화 판정"""
        # xe를 memory로 누적 (단순 저장이 아닌 궤적 형성)
        self.memory_path.extend(xe_list)
        
        # 에너지가 임계치 이하로 떨어지거나 루프가 포화되면 중단 (Attractor 도달)
        is_stable = len(xe_list) == 0 or cycle >= 5
        
        if is_stable:
            log.signal("Attractor Reached. Structure stabilized.")
            return False, ""
        
        # xe(잔여)가 다음 사이클의 압력(re-entry)이 됨
        reentry_payload = f"Tension from {len(xe_list)} residues: {[r.content for r in xe_list]}"
        return True, reentry_payload