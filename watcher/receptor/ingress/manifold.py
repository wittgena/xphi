# watcher.receptor.ingress.manifold
import asyncio
import os
import json
from typing import Optional, Dict, Any

from arch.contract.event.bus import AsyncEventBus
from arch.contract.event.psi import PsiEvent, PsiCarrier
from watcher.plane.emitter import get_emitter

log = get_emitter("ingress.receptor", phase="anchor")

class ManifoldReceptor:
    def __init__(self, bus: AsyncEventBus):
        self.bus = bus
        self.mode = os.environ.get("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        self._bridge = None
        self._server_task: Optional[asyncio.Task] = None

    async def _resolve_fastpath(self):
        """
        [지연 초기화] 외부 주입에 의존하지 않고, 내부적으로 Fast-path(Bridge) 구성이 
        가능한 토폴로지인 경우 스스로 의존성을 끌어와 라우팅 브릿지를 활성화합니다.
        """
        if self.mode not in {"LOCAL_DAEMON", "EMBEDDED_BYPASS"}:
            return

        try:
            from arch.topos.tunnel.factory import TunnelFactory
            from kernel.phase.mesh.router import RoutingPolicyEngine, ClusterStateMesh
            from kernel.phase.mesh.memory import BridgeMemory

            log.info("[Receptor] Bootstrapping internal fast-path routing bridge...")
            broker = await TunnelFactory.get_default()
            engine = RoutingPolicyEngine(broker)
            mesh = ClusterStateMesh(broker)
            
            self._bridge = BridgeMemory.resolve_bridge(self.mode, engine, mesh)
            log.info("[Receptor] Fast-path bridge successfully resolved and armed.")
        except ImportError as e:
            log.warning(f"[Receptor] Could not load fast-path dependencies. Falling back to EventBus only. ({e})")
        except Exception as e:
            log.error(f"[Receptor] Fast-path resolution failed: {e}")

    async def ingest_traffic(self, raw_payload: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        외부에서 유입된 트래픽을 검증, 정규화하고 최적의 경로로 디스패치합니다.
        """
        # 1. 의도(Intent) 추출 및 정규화
        intent = raw_payload.get("intent")
        if not intent:
            intent = f"traffic.unknown.{source}"
            log.debug(f"[Receptor] Intent missing in payload. Assigned default: {intent}")

        # 2. Fast-path (Bridge) 라우팅 평가
        if self._bridge:
            log.debug(f"[Receptor] Fast-path active. Bypassing bus for intent: {intent}")
            try:
                decision = await self._bridge.dispatch(intent=intent, payload=raw_payload)
                return {"status": "processed_fastpath", "result": decision, "intent": intent}
            except Exception as e:
                log.error(f"[Receptor] Fast-path evaluation failed for {intent}: {e}. Falling back to Bus.")
        
        # 3. Message-Driven (EventBus) Fallback
        carrier = PsiCarrier(symbol=intent, kind="ingress.request", payload=raw_payload)
        event = PsiEvent(carrier=carrier)
        
        await self.bus.publish(event)
        log.debug(f"[Receptor] Traffic normalized and published to EventBus: {event.symbol}")
        
        return {"status": "event_published", "psi_symbol": event.symbol}

    # =========================================================================
    # 능동적 리스너 구현부 (Active Listeners)
    # =========================================================================

    async def _handle_ipc_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """LOCAL_DAEMON 토폴로지 시 Unix Domain Socket 요청을 처리하는 핸들러"""
        addr = writer.get_extra_info('peername')
        log.debug(f"[Receptor: IPC] Connection accepted from {addr}")
        try:
            data = await reader.read(4096)
            if data:
                payload = json.loads(data.decode('utf-8'))
                response = await self.ingest_traffic(payload, source="ipc_socket")
                writer.write(json.dumps(response).encode('utf-8'))
                await writer.drain()
        except Exception as e:
            log.error(f"[Receptor: IPC] Error handling request: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def listen(self) -> None:
        """
        토폴로지 환경 변수에 따라 적절한 Ingress 서버를 스스로 구동하고 트래픽 대기를 시작합니다.
        """
        # 리스너 구동 전 Fast-path 자가 구성
        await self._resolve_fastpath()

        if self.mode == "LOCAL_DAEMON":
            sock_path = "/tmp/manifold_receptor.sock"
            if os.path.exists(sock_path):
                os.remove(sock_path)
                
            server = await asyncio.start_unix_server(self._handle_ipc_connection, path=sock_path)
            log.info(f"[Receptor] LOCAL_DAEMON active. Listening on IPC Socket: {sock_path}")
            
            async with server:
                await server.serve_forever()

        elif self.mode == "KUBE_GRPC":
            # 실제 gRPC 서버 구동 로직으로 대체될 영역
            log.info("[Receptor] KUBE_GRPC environment detected. Initializing gRPC Servicer...")
            # e.g., await grpc_server.start()
            await asyncio.Event().wait() 

        else:
            log.info("[Receptor] EMBEDDED_BYPASS mode. Operating purely via direct method invocation (No port bound).")
            await asyncio.Event().wait()

    async def shutdown(self):
        """리스너 및 자원 정리"""
        if self._server_task:
            self._server_task.cancel()
        log.info("[Receptor] Shutdown complete.")