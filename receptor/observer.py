# receptor.observer
import asyncio
from watchdog.observers import Observer
from resonance.receptor.source import SourceTracer, FieldKernel
from resonance.surface.topos import PhaseSurface
from resonance.surface.sink import RedisSink 

async def bootstrap_receptor(watch_dir: str = "./src"):
    """
    @desc: Receptor Bootstrap Loop
    @flow: Start Sink → Boot Kernel → Mount Watchdog → Infinite Observe
    """
    # 1. 인프라 및 도메인 표면 연결 (Dependency Injection)
    sink = RedisSink() 
    surface = PhaseSurface(sink)

    # 2. 내부 핵(Kernel) 기동
    kernel = FieldKernel(surface)
    await kernel.start_daemons()

    # 3. 환경 센서(Watchdog) 마운트
    # 현재 실행 중인 asyncio 이벤트 루프를 Tracer에 주입
    main_loop = asyncio.get_running_loop()
    event_handler = SourceTracer(surface, main_loop)
    
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    
    current_phase = await surface.get_current_phase()
    print(f"Receptor active -> observing {watch_dir} (Φ={current_phase})")

    # 4. 자율 루프 유지
    try:
        # 비동기 환경을 블로킹하지 않고 시스템 무한 유지
        while True:
            await asyncio.sleep(3600) 
    except asyncio.CancelledError:
        print("\n[Rupture] Receptor shutting down...")
    finally:
        observer.stop()
        observer.join()
        await sink.close()
        print("Receptor gracefully closed.")

if __name__ == "__main__":
    try:
        # 단일 진입점으로 비동기 루프 실행
        asyncio.run(bootstrap_receptor())
    except KeyboardInterrupt:
        # 외부 압력(SIGINT)에 의한 종료 처리
        pass