# phase.watcher.field.regime
## @lineage: topos.watcher.field.regime
## @lineage: cognitive.watcher.field.regime
## @lineage: cognitive.field.regime
## @lineage: cognitive.node.regime
import math
import random
from typing import Optional
from arch.contract.registry.unified import contract
from arch.contract.interface import ISystemRegime, IPhaseField, IPhaseAtor
from arch.model.event.psi import PsiEvent

@contract.regime("node.regime")
class NodeRegime(ISystemRegime):
    """
    @role: Rupture 발생 직후, 시스템의 낡은 위상을 태워버리고(Reset) 새로운 질서를 부여
    @flow: XeCont -> Field.commit() -> Regime.modify_field()
    """
    def __init__(self, **kwargs):
        self.params = kwargs

    def modify_field(self, field: IPhaseField) -> None:
        """장의 에너지를 초기화하고 위상을 재배열(Big Bang)"""
        states = field.get_state()
        
        for node_id, data in states.items():
            # 텐션 초기화 (응축된 피로도 해소)
            data["tension"] = 0.0
            
            # 일부 노드의 역할을 강제 변환하거나 위상을 산란시킴
            if data["state"] == "NORMAL":
                # 기존 위상에서 벗어나 새로운 무작위 위상으로 튕겨 나감
                data["phase"] = random.uniform(0, 2 * math.pi)
            elif data["state"] == "REFLECTOR":
                # 리플렉터는 새로운 동기화의 기준점이 됨
                data["phase"] = 0.0  

        # Field의 총 압력을 완전히 진공 상태(0.0)로 초기화
        if hasattr(field, 'pressure'):
            field.pressure = 0.0
            
        print("[Regime] Field collapsed and reformed. Tension reset to 0.0")

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        # 특정 Ator의 제약 조건을 갱신하는 로직
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        # 새로운 Epoch에 맞지 않는 낡은 이벤트(과거의 잔여물)를 소거
        if event.context.get("epoch") != "new":
            # return None  # (필요시 차단)
            pass
        return event