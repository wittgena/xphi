# watcher.kernel.exchange
import math
import random
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field, asdict
from sphere.interface import IDynamicsKernel
from sphere.config import KernelConfig
from contract.registry import contract

@contract.kernel("exahange")
class ExchangeSensor(IDynamicsKernel):
    """Φ-evolution kernel: Market trend and volatility operator"""
    def __init__(self, config: KernelConfig):
        self.config = config
        self.herd_threshold = 0.3 # 군집 행동 임계치

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        
        # 시장의 평균 신념(Price Index) 사전 계산
        avg_phase = sum(d["phase"] for d in states.values()) / total_nodes

        for i_id, i_data in states.items():
            if i_data.get("state") == "ATTRACTOR": # Market Maker
                deltas[i_id] = {"d_phase": i_data["omega"] * dt, "target_tension": 0.0}
                continue

            # 이웃이 아닌 '시장 평균(장)'과의 괴리를 계산 (Mean Reversion vs Trend Following)
            market_diff = avg_phase - i_data["phase"]
            
            # 괴리가 너무 크면 버티지 못하고 시장 평균으로 급격히 끌려감 (Herd Behavior)
            if abs(market_diff) > self.herd_threshold:
                d_phase = (market_diff * self.config.global_coupling) * dt
                new_tension = 0.0 # 순응함으로써 긴장 해소
            else:
                # 자신의 고유 논리 유지, 그러나 긴장도 상승
                d_phase = i_data["omega"] * dt
                new_tension = min(i_data["tension"] + abs(market_diff) * 0.1, 10.0)

            deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}

        return deltas
    
    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        bulls = 0
        bears = 0
        visual = []

        for s in states.values():
            ## 위상을 -1 ~ 1 사이의 포지션 값으로 변환
            position = math.sin(s["phase"]) 
            
            if position > 0.5:
                visual.append('🟢') # 강한 매수
                bulls += 1
            elif position > 0:
                visual.append('↗️') # 약한 매수
                bulls += 1
            elif position > -0.5:
                visual.append('↘️') # 약한 매도
                bears += 1
            else:
                visual.append('🔴') # 강한 매도
                bears += 1
                
        avg_volatility = sum(s['tension'] for s in states.values()) / len(states)
        market_trend = "BULLISH" if bulls > bears else "BEARISH 📉"
        
        status_bar = "".join(visual)
        ## 시장에 맞는 용어(Volatility, Bull/Bear Ratio)로 출력
        return f"Vol(VIX): {avg_volatility:.2f} | {bulls:02d}:{bears:02d} [{market_trend}] | {status_bar}"