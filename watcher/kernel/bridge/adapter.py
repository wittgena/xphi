# watcher.kernel.bridge.adapter
import random
import logging
from typing import List, Dict, Any

from arch.contract.interface import IDynamicsKernel

logger = logging.getLogger("bridge.adapter")

class KernelBoundAdapter:
    def __init__(self, kernel: IDynamicsKernel, initial_nodes: int = 5, rupture_threshold: float = 8.0):
        self.kernel = kernel
        self.rupture_threshold = rupture_threshold
        self.dt = 0.1
        self.states: Dict[str, Dict[str, Any]] = {}
        for i in range(initial_nodes):
            self.states[f"node_{i}"] = {
                "phase": random.uniform(0, 3.14),
                "omega": random.uniform(0.5, 1.5),
                "tension": 0.0,
                "state": "ACTIVE"
            }
        
        self._update_metrics()

    @property
    def topology(self) -> int:
        return len(self.states)

    @property
    def pressure(self) -> float:
        return self._pressure

    def _update_metrics(self):
        if not self.states:
            self._pressure = 0.0
            return
        total_tension = sum(node["tension"] for node in self.states.values())
        self._pressure = total_tension / len(self.states)

    def absorb(self, payloads: List[Any]):
        for item in payloads:
            if not item:
                continue

            # @point 1: PhaseBridge에서 올라온 SIGNAL payload 처리
            if isinstance(item, dict) and item.get("boundary") == "brane_to_kernel":
                target_nodes = random.sample(list(self.states.keys()), k=max(1, len(self.states)//2))
                for node in target_nodes:
                    self.states[node]["tension"] += 3.0  # 긴장도 급증
                    self.states[node]["phase"] += 1.5    # 위상 강제 변이
                
                logger.warning(f"[Adapter.Absorb] Perturbation injected into {len(target_nodes)} nodes from BRANE SIGNAL")
            elif isinstance(item, dict) and "payload" in item:
                target_node = random.choice(list(self.states.keys()))
                self.states[target_node]["tension"] += 0.5

        self._update_metrics()

    def evaluate(self) -> str:
        """@flow: 커널 연산 -> 상태 갱신 -> 임계치 판별 (DEPOSIT or CONTINUE)"""
        deltas = self.kernel.compute_step(self.states, dt=self.dt)
        for node_id, delta in deltas.items():
            if node_id in self.states:
                self.states[node_id]["phase"] = (self.states[node_id]["phase"] + delta.get("d_phase", 0)) % (3.14159 * 2)
                self.states[node_id]["tension"] = delta.get("target_tension", 0.0)

        self._update_metrics()
        if self.pressure > self.rupture_threshold:
            return "DEPOSIT"
        
        return "CONTINUE"

    def commit(self):
        logger.info(f"[Adapter.Commit] Epoch collapse. Realigning phase space. Peak Pressure: {self.pressure:.2f}")
        for node_id in self.states:
            self.states[node_id]["tension"] = 0.0
            self.states[node_id]["omega"] *= random.uniform(0.8, 1.2)  # 새로운 에포크에서의 성질 변화
            
        self._update_metrics()