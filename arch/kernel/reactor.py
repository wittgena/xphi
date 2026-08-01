# arch.kernel.reactor
import asyncio
import sys
import os
import signal
import logging
from typing import Callable, Coroutine, Any, Optional

log = logging.getLogger("kernel.reactor")

class KernelReactor:
    _policy_applied = False
    _pinned_core = None

    @classmethod
    def _apply_kernel_optimizations(cls) -> None:
        """@desc: CPU Pinning 및 최적의 비동기 I/O 정책(uvloop)을 커널 레벨에서 주입"""
        if cls._policy_applied:
            return

        # 1. CPU Pinning (태스크 결속) 적용 (리눅스 전용)
        if hasattr(os, 'sched_setaffinity') and hasattr(os, 'sched_getaffinity'):
            try:
                # OS나 Docker(cgroup)가 허용한 CPU 코어 목록 획득
                available_cores = os.sched_getaffinity(0)
                if available_cores:
                    # 첫 번째 코어에 이 프로세스(격리 단위)를 독점 결속
                    target_core = list(available_cores)[0]
                    os.sched_setaffinity(0, {target_core})
                    cls._pinned_core = target_core
                    log.info(f"[Reactor] ⚡ CPU Affinity Pinned to Core: {target_core}")
            except Exception as e:
                log.warning(f"[Reactor] ⚠️ Failed to pin CPU: {e}")

        # 2. 고성능 I/O 루프 주입 (uvloop = epoll)
        if sys.platform not in ('win32', 'cygwin', 'cli'):
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                log.info("[Reactor] 🚀 UVLoop (epoll) Policy injected.")
            except ImportError:
                log.warning("[Reactor] ⚠️ UVLoop unavailable. Using standard asyncio.")
                
        cls._policy_applied = True

    @classmethod
    def _global_exception_handler(cls, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        msg = context.get("exception", context["message"])
        log.critical(f"[Reactor] 💥 Unhandled exception in event loop: {msg}")

    @classmethod
    async def _lifecycle_manager(
        cls, 
        main_coro_func: Callable[[], Coroutine[Any, Any, None]],
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(cls._global_exception_handler)

        shutdown_event = asyncio.Event()

        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: shutdown_event.set())

        main_task = asyncio.create_task(main_coro_func(), name="Reactor-Main")

        await asyncio.wait(
            [main_task, asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED
        )

        if not main_task.done():
            log.info("\n[Reactor] 🛑 OS Signal received. Initiating graceful shutdown...")
            main_task.cancel()
            try:
                await main_task
            except asyncio.CancelledError:
                pass

        if teardown_hook:
            log.info("[Reactor] 🧹 Executing user-defined teardown hook...")
            try:
                await teardown_hook()
            except Exception as e:
                log.error(f"[Reactor] Error during teardown hook: {e}")

        log.info("[Reactor] 🏁 Lifecycle terminated cleanly.")

    @classmethod
    def ignite(
        cls, 
        main_coro_func: Callable[[], Coroutine[Any, Any, None]],
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> None:
        # [핵심] 커널 최적화 우선 적용
        cls._apply_kernel_optimizations()
        
        policy_name = asyncio.get_event_loop_policy().__class__.__name__
        pin_status = f"Core {cls._pinned_core}" if cls._pinned_core is not None else "Floating"
        log.info(f"[Reactor] Ignition... Loop: {policy_name} | CPU: {pin_status}")
        
        try:
            asyncio.run(cls._lifecycle_manager(main_coro_func, teardown_hook))
        except Exception as e:
            log.critical(f"[Reactor] Kernel Panic: {e}", exc_info=True)
            sys.exit(1)