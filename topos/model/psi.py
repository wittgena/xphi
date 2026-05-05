# topos.model.psi
import math
from dataclasses import dataclass, field
from typing import Any, Optional
from bound.surface.emitter import get_emitter

log = get_emitter("topos.psi")

@dataclass(frozen=True)
class ToposSignature:
    """
    발신지의 '구조(Topology)' 자체를 묘사하는 지문(Signature).
    (이벤트의 타입이나 이름이 아닙니다. 발신 공간의 물리적/논리적 형태입니다.)
    """
    dimension: int      # 차원 (L0, L1, L2, L3 등)
    density: float      # 밀도 (연결된 노드의 수 등)
    is_closed: bool     # 닫힌 계(완전한 클로저)인지 여부

    def resonance_with(self, other: 'ToposSignature') -> float:
        """
        두 구조 간의 '동형성(Isomorphism)'을 계산하여 공명(Resonance) 계수를 반환 (0.0 ~ 1.0)
        구조가 비슷할수록 에너지가 손실 없이 전달됩니다.
        """
        dim_diff = abs(self.dimension - other.dimension)
        den_diff = abs(self.density - other.density)
        
        # 차원과 밀도의 차이가 적을수록 공명률 상승 (임의의 감쇠 함수)
        resonance = math.exp(-(dim_diff + den_diff))
        
        # 둘 다 닫힌 계이거나 열린 계이면 공명 증폭
        if self.is_closed == other.is_closed:
            resonance = min(1.0, resonance * 1.2)
            
        return resonance

@dataclass(frozen=True)
class ToposPsi:
    """기존의 Event를 대체하는 개념. 오로지 출처, 강도, 구조, 잔여물만 존재"""
    origin_id: str                 # 발신지 식별자 (로깅용)
    intensity: float                # 파동의 강도 (0.0 ~ 무한대)
    signature: ToposSignature      # 발신지의 위상 구조
    residue: Any = None            # 튕겨져 나온 데이터 잔여물 (xe)