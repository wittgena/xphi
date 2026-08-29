# xphi.watcher.receptor.kernel
## @lineage: watcher.receptor.kernel
"""
@flow: Environment(Sync) → SourceTracer(Membrane) → ReceptorKernel(Multi-Lens) → Rupture(emit)
Acts as the central observation brain, dynamically adjusting topological structures 
and synchronizing the L0 physical membrane (Immune Anchor).
"""
import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from contextlib import suppress

from xphi.watcher.plane.sink import EmitterSink
from xphi.watcher.plane.metric.trajectory import (
    Point, 
    WindowedTrajectory, 
    DefaultBoundLensStrategy, 
    CoDiffBoundLensStrategy, 
    TopologicalStructure
)
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.receptor.filter import SurvivalAnchor

log = get_emitter("receptor.kernel")

STATE_KEY_PHASE = "meta.self:state:current_phase"
CHANNEL_SIGNAL_MUTATION = "meta.self:signals:phase_mutation"
CHANNEL_PSI_FEEDBACK = "meta.self:signals:psi"
CHANNEL_AUTOSCALER = "system:autoscaler:events"

def build_system_topos() -> List[TopologicalStructure]:
    """Constructs the initial topological structures of the system."""
    structures = []
    
    arch = []
    with suppress(ImportError): import xphi.arch.model.sensor as m; arch.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.space.topos.tunnel.surface as m; arch.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.space.topos.tunnel.factory as m; arch.append(m.__name__)
    if arch: structures.append(TopologicalStructure(name="arch.topos", members=arch))

    phase = []
    with suppress(ImportError): import xphi.kernel.space.bind.resolver as m; phase.append(m.__name__)
    with suppress(ImportError): import xphi.watcher.receptor.bootstrap as m; phase.append(m.__name__)
    with suppress(ImportError): import xphi.watcher.receptor.kernel as m; phase.append(m.__name__)
    if phase: structures.append(TopologicalStructure(name="phase.runtime", members=phase))

    watcher = []
    with suppress(ImportError): import xphi.kernel.resonance as m; watcher.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.dphi.ledger.consensus as m; watcher.append(m.__name__)
    with suppress(ImportError): import xphi.kernel.singularity as m; watcher.append(m.__name__)
    if watcher: structures.append(TopologicalStructure(name="watcher.kernel", members=watcher))

    return structures


class ReceptorKernel:
    def __init__(
        self, 
        sink: EmitterSink, 
        window_steps: int = 14, 
        structures: Optional[List[TopologicalStructure]] = None,
        immune_anchor: Optional[SurvivalAnchor] = None
    ):
        self.sink = sink
        self.window_steps = window_steps
        self.structures = structures or []
        self.immune_anchor = immune_anchor
        
        self.kinematic_lens = DefaultBoundLensStrategy(preset_name="tail_risk")
        self.codiff_lens = CoDiffBoundLensStrategy(diff_threshold=0.1)
        self.trajectory_buffer: Dict[str, List[Point]] = {}
        self.last_known_values: Dict[str, float] = {}

        # Synchronize initial topologies to the L0 Membrane
        self._sync_immune_memory()

    def _sync_immune_memory(self):
        """Synchronizes mounted topological structures to the L0 Membrane."""
        if not self.immune_anchor:
            return
            
        # Base intents required for internal communication and hot-reloads
        valid_intents: Set[str] = {"system_health_probe", "reload", "core_genesis"}
        
        # Dynamically inject known spatial topologies
        for struct in self.structures:
            valid_intents.add(struct.name)
            
        self.immune_anchor.update_topologies(valid_intents)
        log.debug(f"[ImmuneSync] Membrane synchronized with {len(valid_intents)} topological identities.")

    async def reload_system_structures(self, new_structures: List[TopologicalStructure]):
        """Dynamically updates structures and realigns the physical barrier."""
        self.structures = new_structures
        self._sync_immune_memory()
        log.info("[Topology] Immune memory synchronized with physical structure mutation.")

    def _get_structure_for(self, signal_id: str) -> Optional[TopologicalStructure]:
        for struct in self.structures:
            if signal_id in struct.members:
                return struct
        return None

    def _calculate_structure_center(self, structure: TopologicalStructure) -> Optional[float]:
        active_vals = [
            self.last_known_values[m] for m in structure.members 
            if m in self.last_known_values
        ]
        if not active_vals:
            return None
        return sum(active_vals) / len(active_vals)

    async def get_current_phase(self) -> str:
        val = await self.sink.get_control_flag(STATE_KEY_PHASE)
        return val or "Φ0"

    async def watch_mutations(self):
        """Subscribe to environmental mutations and loads from SourceTracer/Workers."""
        async for msg in self.sink.subscribe(CHANNEL_SIGNAL_MUTATION):
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8')
                
            if isinstance(msg, str):
                with suppress(json.JSONDecodeError):
                    msg = json.loads(msg)
            
            if isinstance(msg, dict):
                signal_id = msg.get("signal_id")
                value = msg.get("value")
                if signal_id and value is not None:
                    await self._ingest_and_evaluate(signal_id, float(value))

    async def _ingest_and_evaluate(self, signal_id: str, current_value: float):
        """Evaluate raw signals through Kinematic and CoDiff lenses."""
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
        """Standardize and emit spatial rupture events."""
        trace_record = {
            "event": "xphi_structure_event",
            "rupture_type": rupture_type,
            "signal": signal_id,
            "structure": structure_name,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }
        await self.sink.tunnel.publish(CHANNEL_AUTOSCALER, json.dumps(trace_record))

    async def emit_analysis_event(self, payload: dict):
        """Emit meta-events like source code mutations to the feedback loop."""
        merged_payload = payload.copy() if payload else {}
        merged_payload.update({
            "event": "xphi_analysis_event",
            "weight": 1,
            "ts": time.time()
        })
        log.info(f"Ψ emit → {merged_payload}")
        await self.sink.publish(CHANNEL_PSI_FEEDBACK, json.dumps(merged_payload))

    async def watch_psi_feedback(self):
        """Monitor system re-entry trajectories."""
        async for msg in self.sink.subscribe(CHANNEL_PSI_FEEDBACK):
            log.debug(f"🌀 [ReceptorKernel] Re-entry Ψ′ feedback → {msg}")

    async def start_daemons(self):
        """Bootstrap internal observation daemons."""
        asyncio.create_task(self.watch_mutations())
        asyncio.create_task(self.watch_psi_feedback())