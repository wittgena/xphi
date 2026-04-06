# system.surface.exporter
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from bridge.client.prom import PrometheusClient
from bridge.interface.bus import AsyncEventBus
from system.surface.renderer import Renderer
from system.receptor.prom import PromReceptor
from rhythm.system.loop import system_tick_loop

client = PrometheusClient("http://prometheus:9090")
receptor = PromReceptor("main", client)
event_bus = AsyncEventBus()
renderer = Renderer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus.subscribe(receptor)
    tick_task = asyncio.create_task(system_tick_loop(event_bus))
    yield
    
    tick_task.cancel()
    try:
        await tick_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.get("/metrics/metalog")
def get_metrics_receptor():
    state = receptor.state
    return Response(content=renderer.render(state).strip(), media_type="text/plain")

def main():
    """@entry: standalone runtime"""
    import uvicorn
    uvicorn.run(
        "loop.exporter.metrics:app",
        host="0.0.0.0",
        port=9100,
        reload=False
    )

if __name__ == "__main__":
    main()