# phase.receptor.tracer.source
"""@flow: Environment(Sync) → SourceTracer(Membrane) → Ψ(PhaseSurface) → TraceKernel(Lens) → Rupture(emit)"""
import time
import asyncio
from datetime import datetime
from watchdog.events import FileSystemEventHandler
from typing import Dict, List
from session.resonance.surface.topos import PhaseSurface
from phase.receptor.lens.trajectory import Point, WindowedTrajectory, DefaultBoundLensStrategy

class TracerSource(FileSystemEventHandler):
    """@desc: 물리적 파일 시스템의 변이를 감지하여 PhaseSurface에 이산적 틱(Tick)을 주입"""
    def __init__(self, surface: PhaseSurface, loop: asyncio.AbstractEventLoop):
        self.surface = surface
        self.loop = loop  
        self.last_trigger = 0

    def on_modified(self, event):
        if time.time() - self.last_trigger < 2.0:
            return

        if event.src_path.endswith(".java") or event.src_path.endswith(".dsl"):
            print(f"\n✨ [SourceTracer] Physical mutation detected → {event.src_path}")
            self.last_trigger = time.time()
            
            ## [Topological Shift] 단순 이벤트 문자열이 아닌, TraceKernel이 
            ## 궤적을 그릴 수 있는 수치적 파동(value) 형태로 투척합니다.
            payload = {
                "signal_id": "code_mutation_pressure",
                "value": 1.0  # 변이의 강도 (필요시 파일 diff 사이즈 등으로 치환 가능)
            }
            
            asyncio.run_coroutine_threadsafe(
                self.surface.emit_psi("xphi_analysis_event", payload=payload),
                self.loop
            )