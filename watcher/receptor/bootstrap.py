# watcher.receptor.bootstrap
## @lineage: phase.runtime.daemon.receptor.bootstrap
## @lineage: phase.runtime.receptor.bootstrap
"""@flow: Sink(망) → Surface(위상장) → ReceptorKernel(다중 렌즈) → SourceTracer(감각/방어) 마운트"""
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from typing import List

from arch.topos.tunnel.factory import UniversalFacade

from phase.bind.resolver import find_current_self
from watcher.receptor.topos import ReceptorTopos, build_system_topos
from watcher.receptor.kernel import ReceptorKernel
from watcher.plane.sink import TunnelSink 

from watcher.tracer.source import TracerSource
from watcher.plane.metric.trajectory import TopologicalStructure
from watcher.plane.emitter import get_emitter

log = get_emitter("receptor.bootstrap")

SELF_ROOT = find_current_self()

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

if __name__ == "__main__":
    pass