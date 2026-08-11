# watcher.receptor.bootstrap
import asyncio
import json
import time
import sys
import importlib
import traceback
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dataclasses import asdict  # [추가] 안전한 JSON 직렬화를 위한 표준 라이브러리

from arch.contract.discovery import discover_modules
from arch.topos.tunnel.factory import UniversalFacade
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from arch.contract.event.next import next_id

from kernel.bind.resolver import find_current_self
from watcher.plane.sink import TunnelSink 
from watcher.plane.emitter import get_emitter
from watcher.receptor.kernel import ReceptorKernel, build_system_topos

log = get_emitter("receptor.bootstrap")

SELF_ROOT = find_current_self()

class TracerSource(FileSystemEventHandler):
    def __init__(self, kernel: ReceptorKernel, loop: asyncio.AbstractEventLoop, watch_dir: str, tunnel: UniversalFacade):
        self.kernel = kernel
        self.loop = loop  
        self.watch_dir = Path(watch_dir).resolve()
        self.last_trigger = 0
        self.tunnel = tunnel

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
                
                # 1. Master(Receptor) 메모리 공간 갱신
                importlib.reload(sys.modules[module_fqn])
                log.info(f"[Modification] {module_fqn} successfully integrated into Master Runtime.")
                
                # 2. Worker 프로세스들에게 동기화 브로드캐스트 (Distributed Hot-Reload)
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
                
                # [핵심 수정] 중첩된 객체(PsiCarrier 등)의 안전한 직렬화를 위해 asdict 사용
                try:
                    event_data = asdict(sync_event)
                except TypeError:
                    # dataclass가 아닐 경우를 대비한 Fallback (방어 코드)
                    event_data = sync_event.__dict__
                    if hasattr(event_data.get('carrier'), '__dict__'):
                        event_data['carrier'] = event_data['carrier'].__dict__
                
                asyncio.run_coroutine_threadsafe(
                    self.tunnel.state_store.xadd("runtime:bus:stream", {"data": json.dumps(event_data)}),
                    self.loop
                )
                
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
            self.kernel.emit_analysis_event(payload),
            self.loop
        )


async def receptor_bootstrap(tunnel: UniversalFacade, watch_dir: str = SELF_ROOT):
    sink = TunnelSink(tunnel=tunnel) 
    system_topos = build_system_topos()
    kernel = ReceptorKernel(
        sink=sink, 
        window_steps=4, 
        structures=system_topos
    )

    main_loop = asyncio.get_running_loop()
    
    # Worker로 이벤트를 전파할 수 있도록 tunnel 객체 주입
    event_handler = TracerSource(kernel, main_loop, watch_dir=watch_dir, tunnel=tunnel)
    
    # ---------------------------------------------------------
    # Receptor 구동 시 최초 1회 전체 시스템 스캔 수행
    # ---------------------------------------------------------
    log.info(f"\n[Pre-Flight] Initiating system-wide module discovery at {watch_dir}...")
    discovery_start = time.time()
    
    # Block 방식으로 실행 (부트스트랩 단계이므로 비동기 루프 시작 전 안전하게 수행)
    discover_modules(Path(watch_dir))
    
    elapsed = time.time() - discovery_start
    log.info(f"[Pre-Flight] Discovery complete in {elapsed:.2f}s. Topology manifest ready.")
    # ---------------------------------------------------------

    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    log.info(f"\n[Singularity] Physical Membrane Ignited -> observing {watch_dir}")

    try:
        while True:
            try:
                await kernel.start_daemons()
                
                current_phase = await kernel.get_current_phase()
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