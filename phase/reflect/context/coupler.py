# phase.reflect.context.coupler
## @lineage: phase.reflect.coupler
import asyncio
from typing import Optional

from arch.contract.event.psi import PsiEvent, PsiCarrier
from watcher.kernel.state.aggregator import KernelStateAggregator, InternalContext
from phase.runtime.interpreter import PhaseJudgment
from phase.reflect.context.worker import ContextWorker
from watcher.plane.emitter import get_emitter

class ContextCoupler:
    def __init__(self, aggregator: KernelStateAggregator, worker: ContextWorker):
        self.aggregator = aggregator
        self.worker = worker
        self.tension_queue = asyncio.Queue()
        self.log = get_emitter("context.coupler", phase="BRIDGE")
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self.running = True
        self._task = asyncio.create_task(self._consume_loop(), name="ContextCoupler")
        self.log.info("Reflect Coupler bound and listening.")

    async def stop(self):
        self.running = False
        await self.tension_queue.put(None)
        if self._task:
            await self._task

    def ingest(self, psi: PsiCarrier, judgment: PhaseJudgment):
        if judgment.is_resonance:
            self.tension_queue.put_nowait((psi, judgment))
            self.log.trace(f"Psi({psi.symbol}) crossed the boundary. Tension queued.")
        else:
            self.log.trace(f"Psi({psi.symbol}) dropped by interference.")

    async def _consume_loop(self):
        while self.running:
            try:
                payload = await self.tension_queue.get()
                if payload is None:
                    break
                
                psi, judgment = payload
                self.log.info(f"Aggregating internal state for Psi({psi.symbol})...")
                internal_ctx: InternalContext = await self.aggregator.build_context(psi)
                self.log.info("Projecting context to Cognitive Worker...")
                await self.worker.process(internal_ctx)
                self.tension_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Coupler processing error: {e}")