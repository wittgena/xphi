# arch.network.bridge
## @lineage: phi.net.bridge.rpc
import uuid
from typing import Any
from arch.network.channel.pipeline import DuplexChannel, ChannelContext
from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("rpc.bridge")

class FlowPropagator(DuplexChannel):
    def __init__(self, agent_id: str = "SERVER_SIDE"):
        self.agent_id = agent_id

    async def channel_active(self, ctx: ChannelContext):
        ## 연결 성립 시 고유 flow_id 부여 및 AttributeMap 저장
        flow_id = f"flow_{self.agent_id}_{uuid.uuid4().hex[:8]}"
        ctx.set_attr("flow_id", flow_id)
        
        with flow_scope(flow_id=flow_id, phase="NET_ACTIVE", agent_id=self.agent_id):
            log.info("채널 연결 완료 (Flow 생성됨)")
            await ctx.fire_channel_active()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        flow_id = ctx.get_attr("flow_id", "UNKNOWN_FLOW")
        ## 수신 이벤트를 flow_scope로 래핑하여 하위 핸들러로 전달
        with flow_scope(flow_id=flow_id, phase="NET_RX", agent_id=self.agent_id):
            await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        flow_id = ctx.get_attr("flow_id", "UNKNOWN_FLOW")
        ## 송신 이벤트를 flow_scope로 래핑하여 Transport로 전달
        with flow_scope(flow_id=flow_id, phase="NET_TX", agent_id=self.agent_id):
            await ctx.fire_write(msg)

class RpcBridge(DuplexChannel):
    """이벤트 기반 파이프라인 위에서 순차적 RPC(Request-Response)를 가능하게 하는 브릿지"""
    def __init__(self):
        self.pending_requests = {}
        self._seq = 0
        self.ctx = None  # 파이프라인이 연결되면 활성화됨

    async def channel_active(self, ctx: ChannelContext):
        self.ctx = ctx
        await ctx.fire_channel_active()

    async def request(self, payload: dict, timeout: float = 5.0) -> dict:
        if not self.ctx:
            raise ConnectionError("Pipeline is not active yet.")
            
        self._seq += 1
        req_id = f"req_{self._seq}"
        payload["_req_id"] = req_id
        
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_requests[req_id] = fut
        
        await self.ctx.fire_write(payload)
        return await asyncio.wait_for(fut, timeout=timeout)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and "_req_id" in msg:
            req_id = msg["_req_id"]
            if req_id in self.pending_requests:
                # 대기 중인 Future(시나리오)에 응답을 꽂아주고 흐름 재개
                self.pending_requests.pop(req_id).set_result(msg)
                return
        await ctx.fire_channel_read(msg)