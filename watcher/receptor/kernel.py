# watcher.receptor.kernel
"""@flow: Environment(Sync) → SourceTracer(Membrane) → Ψ(PhaseSurface) → ReceptorKernel(Multi-Lens) → Rupture(emit)"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from watcher.receptor.topos import ReceptorTopos
from watcher.plane.metric.trajectory import (
    Point, 
    WindowedTrajectory, 
    DefaultBoundLensStrategy, 
    CoDiffBoundLensStrategy, 
    TopologicalStructure
)

class ReceptorKernel:
    def __init__(self, surface: ReceptorTopos, window_steps: int = 14, structures: List[TopologicalStructure] = None):
        self.surface = surface
        self.window_steps = window_steps
        self.structures = structures or []
        
        self.kinematic_lens = DefaultBoundLensStrategy(preset_name="tail_risk")
        self.codiff_lens = CoDiffBoundLensStrategy(diff_threshold=0.1)
        self.trajectory_buffer: Dict[str, List[Point]] = {}
        self.last_known_values: Dict[str, float] = {}

    def _get_structure_for(self, signal_id: str) -> Optional[TopologicalStructure]:
        """해당 신호가 속한 위상 구조(Φ)를 반환"""
        for struct in self.structures:
            if signal_id in struct.members:
                return struct
        return None

    def _calculate_structure_center(self, structure: TopologicalStructure) -> Optional[float]:
        """구조 내 멤버들의 현재 상태(LKV) 평균을 계산하여 중심점 반환"""
        active_vals = [
            self.last_known_values[m] for m in structure.members 
            if m in self.last_known_values
        ]
        if not active_vals:
            return None
        return sum(active_vals) / len(active_vals)

    async def watch_mutations(self):
        """SourceTracer로부터 유입되는 환경 변이를 구독"""
        async for msg in self.surface.sink.subscribe(self.surface.signal_channel):
            signal_id = msg.get("signal_id")
            value = msg.get("value")
            if signal_id and value is not None:
                await self._ingest_and_evaluate(signal_id, float(value))

    async def _ingest_and_evaluate(self, signal_id: str, current_value: float):
        now = datetime.now()
        
        self.last_known_values[signal_id] = current_value
        if signal_id not in self.trajectory_buffer:
            self.trajectory_buffer[signal_id] = []
            
        buffer = self.trajectory_buffer[signal_id]
        buffer.append(Point(timestamp=now, value=current_value))
        
        if len(buffer) > self.window_steps:
            buffer.pop(0)
            
        if len(buffer) < self.window_steps:
            return

        window = WindowedTrajectory(
            identity=signal_id,
            start_time=buffer[0].timestamp,
            end_time=buffer[-1].timestamp,
            points=buffer
        )

        k_scan = self.kinematic_lens.scan(window)
        if k_scan["status"] == "valid":
            metrics = k_scan["metrics"]
            
            # [개선] 임계치에 따른 Scale-Out(Tension High) / Scale-In(Flatline) 상태 분리 도출
            is_high_tension = metrics.get("trend", 0) > 0.8
            is_flatlined = (metrics.get("mean", 1.0) == 0.0) and (metrics.get("volatility", 1.0) == 0.0)
            is_volatile = metrics.get("volatility", 0) >= 0.05
            
            if is_high_tension:
                await self._emit_rupture("KINEMATIC_TENSION_HIGH", signal_id, metrics)
            elif is_flatlined:
                await self._emit_rupture("KINEMATIC_FLATLINE", signal_id, metrics)
            elif is_volatile:
                await self._emit_rupture("KINEMATIC_VOLATILITY", signal_id, metrics)

        structure = self._get_structure_for(signal_id)
        if structure:
            struct_val = self._calculate_structure_center(structure)
            if struct_val is not None:
                struct_window = WindowedTrajectory(
                    identity=structure.name,
                    start_time=buffer[0].timestamp,
                    end_time=now,
                    points=[Point(timestamp=now, value=struct_val)]
                )
                
                c_scan = self.codiff_lens.scan(window, struct_window)
                if c_scan["status"] == "valid" and c_scan.get("is_ruptured"):
                    await self._emit_rupture("CO_DIFF", signal_id, c_scan["metrics"], structure.name)

    async def _emit_rupture(self, rupture_type: str, signal_id: str, metrics: dict, structure_name: str = None):
        """파열/평탄화 이벤트 규격화 및 발행"""
        print(f"\n⚠️ [ReceptorKernel] {rupture_type} Event: '{signal_id}'")
        
        trace_record = {
            "rupture_type": rupture_type,
            "signal": signal_id,
            "structure": structure_name,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }
        await self.surface.emit_psi("xphi_structure_event", payload=trace_record)

    async def watch_psi_feedback(self):
        """시스템 재진입(Re-entry) 궤적 감시"""
        async for msg in self.surface.sink.subscribe(self.surface.psi_channel):
            print(f"🌀 [ReceptorKernel] Re-entry Ψ′ feedback → {msg}")

    async def start_daemons(self):
        """데몬 부트스트랩"""
        asyncio.create_task(self.watch_mutations())
        asyncio.create_task(self.watch_psi_feedback())