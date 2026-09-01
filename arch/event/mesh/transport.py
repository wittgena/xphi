# xphi.arch.event.mesh.transport
from __future__ import annotations
import asyncio
from typing import Dict, Set, Optional, Callable, Awaitable
import zenoh
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mesh.transport")

class MeshP2PTransport:
    """
    @xe.desc: Native P2P physical transport layer utilizing Eclipse Zenoh protocol stack.
              Completely decoupled from Domain Bus logic via Callback Injection.
    """
    def __init__(self, listen_port: int = 7447):
        self.listen_port = listen_port
        self.session: Optional[zenoh.Session] = None
        self.ingress_callback: Optional[Callable[[str, bytes], Awaitable[None]]] = None
        self.subscribed_topics: Dict[str, zenoh.Subscriber] = {}
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None

    async def bind_and_start(self, ingress_callback: Callable[[str, bytes], Awaitable[None]], bind_ip: str = "0.0.0.0"):
        """@desc: Initializes the Zenoh node in PEER mode and wires the ingress callback"""
        self.ingress_callback = ingress_callback
        self._async_loop = asyncio.get_running_loop()
        
        ## Zenoh 설정: PEER 모드로 동작하여 중앙 브로커 없이 Mesh 네트워크 형성
        conf = zenoh.Config()
        ## 멀티캐스트 기반 Peer Discovery 및 리스닝 포트 설정
        conf.insert_json5("listen/endpoints", f'["tcp/{bind_ip}:{self.listen_port}"]')
        conf.insert_json5("mode", '"peer"')
        
        ## Zenoh 세션 오픈 (Rust 코어 부팅)
        self.session = zenoh.open(conf)
        peer_id = self.session.info.zid()
        log.info(f"[Transport] Zenoh Physical Mesh activated. ZID (PeerID): {peer_id} | Port: {self.listen_port}")

    async def join_topic(self, topic: str):
        """@desc: Subscribes to a Zenoh key expression (topic)"""
        if topic in self.subscribed_topics:
            return

        def _zenoh_handler(sample: zenoh.Sample):
            """@desc: Internal callback triggered by Zenoh's native background thread"""
            if not self.ingress_callback or not self._async_loop:
                return
            
            # Zenoh는 데이터 중심(Data-Centric) 프로토콜이므로 발신자 ID보다는 데이터 자체(Key)를 중시함.
            # 상위 도메인(SwarmBus)의 시그니처를 맞추기 위해 임시 ID를 부여하거나 attachment에서 추출.
            sender_id = "zenoh-peer"
            raw_bytes = sample.payload
            
            # [Thread Bridge] Rust 스레드 -> Python Asyncio 루프로 안전하게 진입
            asyncio.run_coroutine_threadsafe(
                self.ingress_callback(sender_id, raw_bytes),
                self._async_loop
            )

        if not self.session:
            raise RuntimeError("Zenoh session is not initialized.")

        # Zenoh 구독 선언 (key_expr 매핑)
        subscriber = self.session.declare_subscriber(topic, _zenoh_handler)
        self.subscribed_topics[topic] = subscriber
        log.info(f"[Transport] Joined Zenoh Mesh topic (key_expr): {topic}")

    async def broadcast(self, topic: str, payload_bytes: bytes) -> None:
        """@desc: Projects serialized bytes into the Zenoh physical mesh"""
        if not self.session:
            raise RuntimeError("Zenoh session is not initialized.")
            
        # Zenoh put (발행). 내부적으로 인접한 피어들에게 최단 경로로 멀티캐스트 됨.
        self.session.put(topic, payload_bytes)
        log.trace(f"[Transport] Broadcasted {len(payload_bytes)} bytes to {topic}")

    async def close(self):
        """@desc: Gracefully terminates the physical network connections."""
        if self.session:
            for sub in self.subscribed_topics.values():
                sub.undeclare()
            self.session.close()
            log.info("[Transport] Zenoh session and mesh connections terminated.")