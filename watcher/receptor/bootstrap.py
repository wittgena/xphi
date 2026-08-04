# watcher.receptor.bootstrap
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from typing import List
import time
import sys
import importlib
import traceback
from watchdog.events import FileSystemEventHandler
from typing import Dict, List, Optional
from arch.topos.tunnel.factory import UniversalFacade

from kernel.bind.resolver import find_current_self
from watcher.receptor.topos import ReceptorTopos, build_system_topos
from watcher.receptor.kernel import ReceptorKernel
from watcher.plane.sink import TunnelSink 
from watcher.plane.metric.trajectory import TopologicalStructure
from watcher.plane.emitter import get_emitter

log = get_emitter("receptor.bootstrap")

SELF_ROOT = find_current_self()

class TracerSource(FileSystemEventHandler):
    def __init__(self, surface: ReceptorTopos, loop: asyncio.AbstractEventLoop, watch_dir: str):
        self.surface = surface
        self.loop = loop  
        self.watch_dir = Path(watch_dir).resolve()
        self.last_trigger = 0

    def _resolve_fqn(self, file_path: str) -> str:
        path = Path(file_path).resolve()
        try:
            relative = path.relative_to(self.watch_dir)
            return ".".join(relative.with_suffix("").parts)
        except ValueError:
            return ""

    def on_modified(self, event):
        if time.time() - self.last_trigger < 1.0:
            return

        if not (event.src_path.endswith(".py") or event.src_path.endswith(".kt")):
            return

        self.last_trigger = time.time()
        log.info(f"\n✨ [SourceTracer] Physical mutation detected → {event.src_path}")

        module_fqn = self._resolve_fqn(event.src_path)
        payload = {"signal_id": "unknown_mutation", "value": 0.0}

        if module_fqn and module_fqn in sys.modules:
            try:
                log.info(f"[Plasticity] Re-aligning topology for: {module_fqn}")
                importlib.reload(sys.modules[module_fqn])
                log.info(f"[Modification] {module_fqn} successfully integrated into Runtime.")
                
                payload = {
                    "signal_id": "topology_reloaded",
                    "value": 1.0,  # 긍정적 결합 파동
                    "module": module_fqn
                }
            except Exception as e:
                log.info(f"[Cleavage] Critical syntax/logic error in {module_fqn}.")
                log.info(f"  ↳ System protected. Malformed phase rejected.")
                log.info(traceback.format_exc()) # 파지의 시체를 로그로만 출력하고 런타임은 보존
                
                payload = {
                    "signal_id": "mutation_rejected",
                    "value": -1.0, # 부정적 거부 파동 (Tension 상승)
                    "error": str(e)
                }
        else:
            log.info(f"[Genesis] New structure detected: {module_fqn or event.src_path}")
            payload = {"signal_id": "new_structure_detected", "value": 0.5, "module": module_fqn}

        asyncio.run_coroutine_threadsafe(
            self.surface.emit_psi("xphi_analysis_event", payload=payload),
            self.loop
        )

async def receptor_bootstrap(tunnel: UniversalFacade, watch_dir: str = SELF_ROOT):
    sink = TunnelSink(tunnel=tunnel) 
    surface = ReceptorTopos(sink)

    main_loop = asyncio.get_running_loop()
    event_handler = TracerSource(surface, main_loop, watch_dir=watch_dir)
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    log.info(f"\n[Singularity] Physical Membrane Ignited -> observing {watch_dir}")

    try:
        while True:
            try:
                system_topos = build_system_topos()
                kernel = ReceptorKernel(
                    surface=surface, 
                    window_steps=14, 
                    structures=system_topos
                )
                await kernel.start_daemons()
                
                current_phase = await surface.get_current_phase()
                log.info(f"[Topology] Mounted structures: {[s.name for s in system_topos]} (Φ={current_phase})")
                while True:
                    await asyncio.sleep(3600) 
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"[Rupture] Internal anomaly detected: {e}. Membrane remains as a zombie. Renewing in 5s...")
                await asyncio.sleep(5.0) 
                
    except asyncio.CancelledError:
        log.info("\n[Evaporation] Leadership yielded. Receptor dissolving...")
    finally:
        if observer.is_alive():
            observer.stop()
            observer.join()
        await sink.close()
        log.info("Receptor elegantly evaporated.")