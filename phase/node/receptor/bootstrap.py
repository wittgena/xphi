# phase.node.receptor.bootstrap
import asyncio
from watchdog.observers import Observer
from phase.node.receptor.source import TracerSource
from phase.node.receptor.kernel import TracerKernel
from phase.node.receptor.topos import SurfaceTopos
from phase.topos.surface.sink import RedisSink 

async def receptor_bootstrap(watch_dir: str = "./src"):
    """
    @desc: Receptor Bootstrap Loop (시스템의 자가생성 부팅 시퀀스)
    @flow: Sink(망) → Surface(위상장) → TraceKernel(인지) → SourceTracer(감각) 마운트
    """
    ## 인프라 및 도메인 표면 연결 (Substrate Injection)
    sink = RedisSink() 
    surface = SurfaceTopos(sink)

    ## 내부 핵(Cognitive Kernel) 기동
    ## 과거의 반사 신경(FieldKernel) 대신, 시간을 다루는 렌즈(TraceKernel)를 주입합니다.
    ## 여기서 관점(preset)과 시간의 길이(window_steps)를 결정합니다.
    kernel = TracerKernel(surface, window_steps=14, lens_preset="tail_risk")
    await kernel.start_daemons()

    ## 환경 센서(Watchdog Membrane) 마운트
    ## 물리적 세계의 변이를 비동기 루프로 밀어넣기 위한 브릿지 연결
    main_loop = asyncio.get_running_loop()
    event_handler = TracerSource(surface, main_loop)
    
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    current_phase = await surface.get_current_phase()
    print(f"\n🌀 [Singularity] Receptor Active -> observing {watch_dir} (Φ={current_phase})")

    ## 자율 루프 유지 (Autopoietic Loop)
    try:
        ## 비동기 환경을 블로킹하지 않고 시스템 무한 유지
        while True:
            await asyncio.sleep(3600) 
    except asyncio.CancelledError:
        print("\n⚠️ [Rupture] Receptor shutting down due to internal phase collapse...")
    finally:
        observer.stop()
        observer.join()
        await sink.close()
        print("✨ Receptor gracefully closed.")

if __name__ == "__main__":
    try:
        ## 단일 진입점으로 비동기 우주(Event Loop) 창세
        asyncio.run(receptor_bootstrap())
    except KeyboardInterrupt:
        ## 외부 압력(SIGINT)에 의한 시스템 자발적 붕괴 수용
        pass