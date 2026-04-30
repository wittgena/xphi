# receptor.source
import time
import asyncio
from watchdog.events import FileSystemEventHandler
from resonance.surface.topos import PhaseSurface

class SourceTracer(FileSystemEventHandler):
    """
    @desc: filesystem mutation → semantic signal
    @flow: environment(Sync Thread) → Membrane Bridge → Ψ(Async Loop)
    """
    def __init__(self, surface: PhaseSurface, loop: asyncio.AbstractEventLoop):
        self.surface = surface
        self.loop = loop  # Watchdog 스레드에서 Async 메인 루프로 이벤트를 밀어넣기 위한 브릿지
        self.last_trigger = 0

    def on_modified(self, event):
        if time.time() - self.last_trigger < 2.0:
            return

        if event.src_path.endswith(".java") or event.src_path.endswith(".dsl"):
            print(f"\n✨ mutation detected → {event.src_path}")
            self.last_trigger = time.time()
            
            # [중요] 동기 스레드에서 비동기 코루틴을 안전하게 실행 (Membrane Transport)
            asyncio.run_coroutine_threadsafe(
                self.surface.emit_psi("xphi_analysis_event", weight=1),
                self.loop
            )


class FieldKernel:
    """
    @desc: Manages the autonomous closed-loop of the system.
    Evaluates Φ mutations and applies structural inversions (Ψ′).
    """
    def __init__(self, surface: PhaseSurface):
        self.surface = surface

    async def apply_inversion(self, phase: str):
        """@flow: Φ -> Ψ′"""
        if phase == "∂Φ":
            await self.surface.emit_psi("xphi_new_event", weight=2)
        elif phase == "Φ4":
            await self.surface.emit_psi("xphi_structure_event", weight=0)

    async def watch_mutations(self):
        """인프라 독립적인 Async PubSub 구독"""
        async for message in self.surface.sink.subscribe(self.surface.signal_channel):
            new_phase = message
            print(f"\n🌀 Φ mutation → {new_phase}")
            await self.apply_inversion(new_phase)

    async def watch_psi_feedback(self):
        async for msg in self.surface.sink.subscribe(self.surface.psi_channel):
            print(f"re-entry Ψ′ → {msg}")

    async def start_daemons(self):
        """Boots the background nervous system via Asyncio Tasks."""
        asyncio.create_task(self.watch_mutations())
        asyncio.create_task(self.watch_psi_feedback())