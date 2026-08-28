# xphi.watcher.receptor.bootstrap
## @lineage: watcher.receptor.bootstrap
import asyncio
import json
import time
import sys
import importlib
import traceback
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dataclasses import asdict

from xphi.arch.contract.discovery import discover_modules
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.contract.event.next import next_id

from xphi.kernel.space.bind.resolver import find_current_self
from xphi.watcher.plane.sink import TunnelSink 
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.receptor.kernel import ReceptorKernel, build_system_topos

from xphi.watcher.receptor.audit.filter import (
    SurvivalAnchor, 
    CognitiveMembrane, 
    TunnelL0Interceptor
)

log = get_emitter("receptor.bootstrap")

SELF_ROOT = find_current_self()

class TracerSource(FileSystemEventHandler):
    """Detects physical file mutations and triggers topological realignment."""
    def __init__(self, kernel: ReceptorKernel, loop: asyncio.AbstractEventLoop, watch_dir: str, tunnel: UniversalFacade):
        self.kernel = kernel
        self.loop = loop  
        self.watch_dir = Path(watch_dir).resolve()
        self.last_trigger = 0
        self.tunnel = tunnel

    def _resolve_fqn(self, file_path: str) -> str:
        """Resolve absolute file path to a fully qualified module name."""
        path = Path(file_path).resolve()
        try:
            relative = path.relative_to(self.watch_dir)
            return ".".join(relative.with_suffix("").parts)
        except ValueError:
            return ""

    def on_modified(self, event):
        """Handle physical file modification events with debounce."""
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
                
                # 1. Refresh Master (Receptor) memory space
                importlib.reload(sys.modules[module_fqn])
                log.info(f"[Modification] {module_fqn} successfully integrated into Master Runtime.")
                
                # 2. Broadcast hot-reload event to all Worker processes
                sync_event = PsiEvent(
                    event_id=next_id(), 
                    parent_id=None, 
                    source_id="receptor", 
                    scope="GLOBAL", 
                    tick=0, 
                    phase_id=0, 
                    context={},
                    carrier=PsiCarrier(
                        kind="system:topology", 
                        tag="reload", 
                        payload={"module_fqn": module_fqn}, 
                        carrier_type=CarrierType.FIXED
                    )
                )
                
                # Ensure safe serialization of nested dataclasses
                try:
                    event_data = asdict(sync_event)
                except TypeError:
                    # Fallback for non-dataclass edge cases
                    event_data = sync_event.__dict__
                    if hasattr(event_data.get('carrier'), '__dict__'):
                        event_data['carrier'] = event_data['carrier'].__dict__
                
                asyncio.run_coroutine_threadsafe(
                    self.tunnel.state_store.xadd("runtime:bus:stream", {"data": json.dumps(event_data)}),
                    self.loop
                )
                
                payload = {
                    "signal_id": "topology_reloaded",
                    "value": 1.0,  # Positive binding wave
                    "module": module_fqn
                }
            except Exception as e:
                log.info(f"[Cleavage] Critical syntax/logic error in {module_fqn}.")
                log.info(f"  ↳ System protected. Malformed phase rejected.")
                log.info(traceback.format_exc()) 
                
                payload = {
                    "signal_id": "mutation_rejected",
                    "value": -1.0, # Negative rejection wave (Tension spike)
                    "error": str(e)
                }
        else:
            log.info(f"[Genesis] New structure detected: {module_fqn or event.src_path}")
            payload = {"signal_id": "new_structure_detected", "value": 0.5, "module": module_fqn}

        # Emit the analysis result to the Kernel
        asyncio.run_coroutine_threadsafe(
            self.kernel.emit_analysis_event(payload),
            self.loop
        )


async def receptor_bootstrap(tunnel: UniversalFacade, watch_dir: str = SELF_ROOT):
    immune_anchor = SurvivalAnchor()
    membrane = CognitiveMembrane(immune_anchor)
    tunnel_filter = TunnelL0Interceptor(membrane)
    
    if hasattr(tunnel, "register_ingress_filter"):
        tunnel.register_ingress_filter(tunnel_filter.intercept)
        log.info("[Boot] L0 Tunnel Membrane physically attached to UniversalFacade.")

    sink = TunnelSink(tunnel=tunnel) 
    system_topos = build_system_topos()
    kernel = ReceptorKernel(
        sink=sink, 
        window_steps=4, 
        structures=system_topos,
        immune_anchor=immune_anchor  
    )

    main_loop = asyncio.get_running_loop()
    event_handler = TracerSource(kernel, main_loop, watch_dir=watch_dir, tunnel=tunnel)
    
    log.info(f"\n[Pre-Flight] Initiating system-wide module discovery at {watch_dir}...")
    discovery_start = time.time()
    
    # Blocking call to guarantee safe discovery before the async loop takes over
    discover_modules(Path(watch_dir))
    
    elapsed = time.time() - discovery_start
    log.info(f"[Pre-Flight] Discovery complete in {elapsed:.2f}s. Topology manifest ready.")

    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    log.info(f"\n[Singularity] Receptor Ignited -> Observing {watch_dir}")

    try:
        while True:
            try:
                # Start internal phase and feedback daemons
                await kernel.start_daemons()
                
                current_phase = await kernel.get_current_phase()
                log.info(f"[Topology] Mounted structures: {[s.name for s in system_topos]} (Φ={current_phase})")
                
                # Keep the main coroutine alive
                while True:
                    await asyncio.sleep(3600) 
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"[Rupture] Internal anomaly detected: {e}. Membrane remains active. Renewing in 5s...")
                await asyncio.sleep(5.0) 
                
    except asyncio.CancelledError:
        log.info("\n[Evaporation] Leadership yielded. Receptor dissolving...")
    finally:
        if observer.is_alive():
            observer.stop()
            observer.join()
        await sink.close()
        log.info("Receptor elegantly evaporated.")