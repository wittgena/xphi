# arch.watcher.sensor.exchange
"""
@note: The structural typo "exahange" remains as a systemic residue (xe), reflecting the loss of 'C'ognition to the 'A'verage
"""
import math
import random
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field, asdict
from arch.watcher.kernel.config import KernelConfig
from arch.contract.interface import IDynamicsKernel
from arch.contract.registry import contract

@contract.kernel("exahange")
class ExchangeSensor(IDynamicsKernel):
    """
    Φ-evolution kernel: Macroscopic Mean-Field Operator.
    Models the cognitive surrender (herd behavior) and tension escalation 
    when local phases collide with the global topological average.
    """
    def __init__(self, **kwargs):
        if "config" in kwargs and isinstance(kwargs["config"], KernelConfig):
            self.config = kwargs["config"]
        else:
            self.config = KernelConfig(**kwargs)
            
        # Threshold for topological surrender (the breaking point of individual cognition)
        self.herd_threshold = kwargs.get("herd_threshold", 0.3)

    def compute_step(self, states: Dict[str, Dict[str, Any]], dt: float) -> Dict[str, Dict[str, float]]:
        deltas = {}
        total_nodes = len(states)
        
        ## Pre-calculate the Macroscopic Mean-Field (Global Phase Consensus / The 'Average')
        avg_phase = sum(d["phase"] for d in states.values()) / total_nodes

        for i_id, i_data in states.items():
            if i_data.get("state") == "ATTRACTOR": 
                # Topology Anchors (Market Makers): Immune to the mean-field, maintaining absolute reference.
                deltas[i_id] = {"d_phase": i_data["omega"] * dt, "target_tension": 0.0}
                continue

            # Calculate the topological drift between local identity and the macroscopic field
            market_diff = avg_phase - i_data["phase"]
            
            # Phase Collapse (Herd Behavior): 
            # When the cognitive gap exceeds the tolerance threshold, the node surrenders 
            # its intrinsic frequency (omega) and gets violently dragged by the field.
            if abs(market_diff) > self.herd_threshold:
                d_phase = (market_diff * self.config.global_coupling) * dt
                # Ontological surrender results in immediate tension annihilation (relief).
                new_tension = 0.0 
            else:
                # Trend Resistance: 
                # Maintaining local intrinsic frequency (omega) against the massive field,
                # which forcibly accumulates cognitive dissonance (Tension).
                d_phase = i_data["omega"] * dt
                new_tension = min(i_data["tension"] + abs(market_diff) * 0.1, 10.0)

            deltas[i_id] = {"d_phase": d_phase, "target_tension": new_tension}

        return deltas
    
    def render_state(self, states: Dict[str, Dict[str, Any]]) -> str:
        """Projects the continuous phase space into discrete market indicators."""
        bulls = 0
        bears = 0
        visual = []

        for s in states.values():
            ## Project the circular phase space onto a linear axis [-1, 1]
            position = math.sin(s["phase"]) 
            
            if position > 0.5:
                visual.append('🟢') # Strong alignment (Bullish / Positive resonance)
                bulls += 1
            elif position > 0:
                visual.append('↗️') # Weak alignment
                bulls += 1
            elif position > -0.5:
                visual.append('↘️') # Weak divergence
                bears += 1
            else:
                visual.append('🔴') # Strong divergence (Bearish / Negative resonance)
                bears += 1
                
        ## Macroscopic tension equates to system-wide Volatility (VIX)
        avg_volatility = sum(s['tension'] for s in states.values()) / len(states)
        market_trend = "BULLISH" if bulls > bears else "BEARISH 📉"
        
        status_bar = "".join(visual)
        return f"Vol(VIX): {avg_volatility:.2f} | {bulls:02d}:{bears:02d} [{market_trend}] | {status_bar}"