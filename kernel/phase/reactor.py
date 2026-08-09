# kernel.phase.reactor
import asyncio
import sys
import os
import signal
import logging
from typing import Callable, Coroutine, Any, Optional

log = logging.getLogger("kernel.reactor")

class PhaseReactor:
    _policy_applied = False
    _pinned_core = None

    @classmethod
    def _apply_kernel_optimizations(cls) -> None:
        if cls._policy_applied:
            return

        # 1. CPU Affinity Pinning (N-Core Lock-free Distribution)
        if hasattr(os, 'sched_setaffinity') and hasattr(os, 'sched_getaffinity'):
            try:
                available_cores = list(os.sched_getaffinity(0))
                if available_cores:
                    # PID 기반 모듈러 연산으로 가용 코어에 프로세스를 자동 분산 할당
                    target_core = available_cores[os.getpid() % len(available_cores)]
                    os.sched_setaffinity(0, {target_core})
                    cls._pinned_core = target_core
                    log.info(f"[Reactor] ⚡ CPU Affinity Pinned to Core: {target_core}")
            except Exception as e:
                log.warning(f"[Reactor] ⚠️ Failed to pin CPU: {e}")

        # 2. UVLoop (epoll) Injection
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
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        shutdown_timeout: float = 10.0
    ) -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(cls._global_exception_handler)
        shutdown_event = asyncio.Event()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except (NotImplementedError, RuntimeError):
                pass

        main_task = asyncio.create_task(main_coro_func(), name="Reactor-Main")
        shutdown_waiter = asyncio.create_task(shutdown_event.wait(), name="Reactor-Shutdown-Waiter")

        try:
            done, pending = await asyncio.wait(
                [main_task, shutdown_waiter],
                return_when=asyncio.FIRST_COMPLETED
            )

            if shutdown_waiter in done:
                log.info("\n[Reactor] 🛑 OS Signal received. Initiating graceful shutdown...")
                main_task.cancel()
                try:
                    await asyncio.wait_for(main_task, timeout=shutdown_timeout)
                except asyncio.TimeoutError:
                    log.warning(f"[Reactor] ⚠️ Main task shutdown timed out ({shutdown_timeout}s). Forcing exit.")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.error(f"[Reactor] 💥 Main task raised exception during shutdown: {e}")
            else:
                shutdown_waiter.cancel()

        except asyncio.CancelledError:
            log.warning("\n[Reactor] 🛑 Reactor itself was cancelled. Cascading shutdown...")
            main_task.cancel()
        finally:
            if teardown_hook:
                log.info("[Reactor] 🧹 Executing user-defined teardown hook...")
                try:
                    await teardown_hook()
                except Exception as e:
                    log.error(f"[Reactor] 💥 Error during teardown hook: {e}")

            log.info("[Reactor] 🏁 Lifecycle terminated cleanly.")

    @classmethod
    def ignite(
        cls, 
        main_coro_func: Callable[[], Coroutine[Any, Any, None]],
        teardown_hook: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        shutdown_timeout: float = 10.0
    ) -> None:
        cls._apply_kernel_optimizations()
        policy_name = asyncio.get_event_loop_policy().__class__.__name__
        pin_status = f"Core {cls._pinned_core}" if cls._pinned_core is not None else "Floating"
        log.info(f"[Reactor] Ignition... Loop: {policy_name} | CPU: {pin_status}")
        
        try:
            asyncio.run(cls._lifecycle_manager(main_coro_func, teardown_hook, shutdown_timeout))
        except KeyboardInterrupt:
            log.info("\n[Reactor] 🛑 KeyboardInterrupt caught at root. Shutdown forced.")
        except Exception as e:
            log.critical(f"[Reactor] Kernel Panic: {e}", exc_info=True)
            sys.exit(1)