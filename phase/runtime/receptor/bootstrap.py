# phase.runtime.receptor.bootstrap
"""@flow: Sink(망) → Surface(위상장) → ReceptorKernel(다중 렌즈) → SourceTracer(감각/방어) 마운트"""
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from typing import List

from phase.bind.resolver import find_current_self
from phase.runtime.receptor.topos import ReceptorTopos, build_system_topos
from phase.runtime.receptor.kernel import ReceptorKernel
from phase.runtime.surface.sink import RedisSink 

from watcher.tracer.source import TracerSource
from watcher.tracer.trajectory import TopologicalStructure
from watcher.plane.emitter import get_emitter

log = get_emitter("receptor.bootstrap")

SELF_ROOT = find_current_self()

async def receptor_bootstrap(watch_dir: str = SELF_ROOT):
    """
    @desc: Receptor Bootstrap Loop (시스템의 자가생성 부팅 시퀀스)
    """
    sink = RedisSink() 
    surface = ReceptorTopos(sink)

    # 안전하게 동적 생성된 위상 구조(Φ) 로드
    system_topos = build_system_topos()

    # 다중 렌즈 커널 마운트 (structures 주입)
    kernel = ReceptorKernel(
        surface=surface, 
        window_steps=14, 
        structures=system_topos
    )
    await kernel.start_daemons()

    # watch_dir를 명시적으로 주입하여 FQN(논리적 위상) 추론이 가능하도록 지원
    main_loop = asyncio.get_running_loop()
    event_handler = TracerSource(surface, main_loop, watch_dir=watch_dir)
    
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    current_phase = await surface.get_current_phase()
    print(f"\n[Singularity] Receptor Active -> observing {watch_dir} (Φ={current_phase})")
    print(f"[Topology] Mounted structures: {[s.name for s in system_topos]}")

    try:
        while True:
            await asyncio.sleep(3600) 
    except asyncio.CancelledError:
        print("\n[Rupture] Receptor shutting down due to internal phase collapse...")
    finally:
        observer.stop()
        observer.join()
        await sink.close()
        print("Receptor gracefully closed.")

if __name__ == "__main__":
    try:
        asyncio.run(receptor_bootstrap())
    except KeyboardInterrupt:
        pass