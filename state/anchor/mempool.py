# xphi.state.anchor.mempool
import os
import json
import uuid
import time
import asyncio
from typing import Any, Dict, Optional, TYPE_CHECKING

import redis.exceptions
from xphi.node.space.tunnel.factory import TunnelFactory, UniversalFacade, UniversalFacadeSync
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.anchor.key import KeyGen

if TYPE_CHECKING:
    from xphi.state.anchor.store import PhaseStore, LogicStream, SealedKernel

log = get_emitter("anchor.mempool", phase="KERNEL")


class MempoolRelay:
    """PROPOSER와 SEALER 간의 분산 Mempool 스트림 중계 및 ACK 동기화 전담 모듈"""
    CONSUMER_GROUP = "anchor_sealer_group"

    def __init__(self, store: "PhaseStore", ack_timeout: float = 4.0):
        self.store = store
        self.ack_timeout = ack_timeout
        self.consumer_name = f"sealer_{os.getpid()}_{uuid.uuid4().hex[:6]}"

        self.stream_logic = KeyGen.mempool_logic()
        self.stream_objects = KeyGen.mempool_objects()
        self.stream_heads = KeyGen.mempool_heads()

        self.sync_tunnel: UniversalFacadeSync = TunnelFactory.get_sync()
        self._consumer_task: Optional[asyncio.Task] = None
        self._running = False

    # [PROPOSER 측] 요청 발송 및 ACK 대기 (Sync / Async)
    def propose_object_sync(self, obj_type: str, obj_hash: str, data: Dict[str, Any]) -> str:
        """_put_object (동기 메서드) 호출 시 SEALER에게 객체 저장을 위임하고 ACK를 대기"""
        req_id = uuid.uuid4().hex
        payload = {
            "req_id": req_id,
            "type": obj_type,
            "hash": obj_hash,
            "data": json.dumps(data)
        }
        try:
            self.sync_tunnel.stream_produce(self.stream_objects, payload)
            self._wait_ack_sync(req_id, timeout=1.5)
        except Exception as e:
            log.debug(f"[Mempool:PROPOSER] Sync object relay fallback ({obj_type}:{obj_hash[:8]}): {e}")
        return obj_hash

    def propose_head_sync(self, stream_id: str, commit_hash: str) -> None:
        """update_head (동기 메서드) 호출 시 SEALER에게 HEAD 포인터 갱신을 위임하고 ACK를 대기"""
        req_id = uuid.uuid4().hex
        payload = {
            "req_id": req_id,
            "stream_id": stream_id,
            "commit_hash": commit_hash
        }
        try:
            self.sync_tunnel.stream_produce(self.stream_heads, payload)
            self._wait_ack_sync(req_id, timeout=1.5)
        except Exception as e:
            log.debug(f"[Mempool:PROPOSER] Sync head relay fallback ({stream_id}): {e}")

    async def propose_stream_async(self, stream: "LogicStream") -> Optional["SealedKernel"]:
        """propose_and_seal (비동기 메서드) 호출 시 SEALER에게 봉인을 위임하고 SealedKernel ACK를 대기"""
        from xphi.state.anchor.store import SealedKernel

        req_id = uuid.uuid4().hex
        stream_data = {
            "req_id": req_id,
            "id": str(stream.id),
            "action": str(stream.action),
            "payload": json.dumps(stream.payload) if isinstance(stream.payload, (dict, list)) else str(stream.payload),
            "metadata": json.dumps(stream.metadata) if isinstance(stream.metadata, dict) else str(stream.metadata),
        }

        try:
            async_tunnel: UniversalFacade = await TunnelFactory.get_default()
            await async_tunnel.stream_produce(self.stream_logic, stream_data)
            ack_data = await self._wait_ack_async(async_tunnel, req_id, timeout=self.ack_timeout)
            if ack_data and ack_data.get("status") == "SEALED" and "kernel" in ack_data:
                return SealedKernel(**ack_data["kernel"])
        except Exception as e:
            log.debug(f"[Mempool:PROPOSER] Async stream relay fallback for '{stream.id}': {e}")

        return None

    # [ACK 송수신 헬퍼: LPUSH + BLPOP]
    def _wait_ack_sync(self, req_id: str, timeout: float) -> Optional[Dict[str, Any]]:
        """UniversalFacadeSync.blpop을 사용하여 SEALER의 ACK를 대기"""
        ack_key = KeyGen.ack_channel(req_id)
        try:
            # socket_timeout(5.0s)보다 짧게 설정하여 TimeoutError 방지
            block_sec = min(float(timeout), 3.5)
            res = self.sync_tunnel.blpop([ack_key], timeout=block_sec)
            if res:
                _, raw_val = res
                self.sync_tunnel.delete(ack_key)
                return json.loads(raw_val)
        except redis.exceptions.TimeoutError:
            pass
        except Exception as e:
            log.debug(f"[Mempool:ACK] Sync ACK wait interrupted ({req_id[:8]}): {e}")
        return None

    async def _wait_ack_async(
        self, async_tunnel: UniversalFacade, req_id: str, timeout: float
    ) -> Optional[Dict[str, Any]]:
        """UniversalFacade.blpop을 사용하여 비동기로 SEALER의 ACK를 대기"""
        ack_key = KeyGen.ack_channel(req_id)
        deadline = time.time() + timeout
        try:
            while time.time() < deadline:
                rem = deadline - time.time()
                if rem <= 0:
                    break
                # socket_timeout(5.0s) 미만의 단위로 끊어서 대기
                step_timeout = min(rem, 2.0)
                res = await async_tunnel.blpop([ack_key], timeout=step_timeout)
                if res:
                    _, raw_val = res
                    await async_tunnel.delete(ack_key)
                    return json.loads(raw_val)
        except redis.exceptions.TimeoutError:
            pass
        except Exception as e:
            log.debug(f"[Mempool:ACK] Async ACK wait interrupted ({req_id[:8]}): {e}")
        return None

    async def _send_ack_async(self, async_tunnel: UniversalFacade, req_id: str, result: Dict[str, Any], ttl: int = 30) -> None:
        if not req_id:
            return
        ack_key = KeyGen.ack_channel(req_id)
        try:
            await async_tunnel.lpush(ack_key, json.dumps(result))
            await async_tunnel.expire(ack_key, ttl)
        except Exception as e:
            log.warning(f"[Mempool:SEALER] Failed to send ACK for {req_id[:8]}: {e}")

    # [SEALER 측] 백그라운드 Mempool Consumer 루프
    def start_sealer_consumer(self) -> None:
        if self._running:
            return
        try:
            loop = asyncio.get_running_loop()
            self._running = True
            self._consumer_task = loop.create_task(self._consume_loop())
            log.info(f"[Mempool:SEALER] Background Consumer [{self.consumer_name}] attached.")
        except RuntimeError:
            # __init__ 시점에 아직 이벤트 루프가 돌지 않는 경우, 추후 비동기 호출 시점에 기동됨
            pass

    def ensure_sealer_consumer(self) -> None:
        """이벤트 루프 진입 후 Consumer가 아직 떠 있지 않다면 안전하게 기동합니다."""
        if not self._running or self._consumer_task is None or self._consumer_task.done():
            self.start_sealer_consumer()

    def stop_sealer_consumer(self) -> None:
        self._running = False
        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()

    async def _ensure_groups(self, async_tunnel: UniversalFacade, topics: list[str]) -> None:
        for topic in topics:
            try:
                await async_tunnel.xgroup_create(topic, self.CONSUMER_GROUP, id="0", mkstream=True)
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    async def _consume_loop(self) -> None:
        from xphi.state.anchor.store import LogicStream

        try:
            async_tunnel: UniversalFacade = await TunnelFactory.get_default()
            topics = [self.stream_logic, self.stream_objects, self.stream_heads]
            await self._ensure_groups(async_tunnel, topics)
            stream_dict = {t: ">" for t in topics}
        except Exception as e:
            log.error(f"[Mempool:SEALER] Failed to initialize consumer streams: {e}")
            self._running = False
            return

        while self._running:
            try:
                # 3개 토픽을 단일 xreadgroup 호출로 멀티플렉싱 수신 (block=500ms로 socket_timeout=5.0s 여유 확보)
                events = await async_tunnel.xreadgroup(
                    groupname=self.CONSUMER_GROUP,
                    consumername=self.consumer_name,
                    streams=stream_dict,
                    count=16,
                    block=500
                )
                if not events:
                    continue

                for stream_name, messages in events:
                    for msg_id, fields in messages:
                        await self._handle_message(async_tunnel, stream_name, msg_id, fields, LogicStream)

            except asyncio.CancelledError:
                break
            except redis.exceptions.TimeoutError:
                continue
            except Exception as e:
                log.debug(f"[Mempool:SEALER] Consumer loop warning: {e}")
                await asyncio.sleep(0.2)

    async def _handle_message(
        self,
        async_tunnel: UniversalFacade,
        stream_name: str,
        msg_id: str,
        fields: Dict[str, str],
        logic_stream_cls: Any,
    ) -> None:
        req_id = fields.get("req_id", "")
        try:
            if stream_name == self.stream_objects:
                obj_type = fields["type"]
                data = json.loads(fields["data"])
                obj_hash = self.store._put_object(obj_type, data)
                await self._send_ack_async(async_tunnel, req_id, {"status": "OK", "hash": obj_hash})

            elif stream_name == self.stream_heads:
                self.store.update_head(fields["stream_id"], fields["commit_hash"])
                await self._send_ack_async(async_tunnel, req_id, {"status": "OK"})

            elif stream_name == self.stream_logic:
                if getattr(self.store, "wasm", None) is None:
                    await self._send_ack_async(async_tunnel, req_id, {"status": "NO_WASM"})
                else:
                    raw_payload = fields.get("payload", "{}")
                    raw_meta = fields.get("metadata", "{}")
                    try:
                        parsed_payload = json.loads(raw_payload)
                    except Exception:
                        parsed_payload = raw_payload
                    try:
                        parsed_meta = json.loads(raw_meta)
                    except Exception:
                        parsed_meta = {}

                    ls = logic_stream_cls(
                        id=fields["id"],
                        action=fields.get("action", "default_action"),
                        payload=parsed_payload,
                        metadata=parsed_meta
                    )
                    sealed = await self.store.propose_and_seal(ls)
                    if sealed:
                        await self._send_ack_async(async_tunnel, req_id, {"status": "SEALED", "kernel": sealed.model_dump()})
                    else:
                        await self._send_ack_async(async_tunnel, req_id, {"status": "REJECTED"})
        except Exception as e:
            log.error(f"[Mempool:SEALER] Error processing {stream_name} ({msg_id}): {e}")
            await self._send_ack_async(async_tunnel, req_id, {"status": "ERROR", "msg": str(e)})
        finally:
            await async_tunnel.stream_ack(stream_name, self.CONSUMER_GROUP, msg_id)