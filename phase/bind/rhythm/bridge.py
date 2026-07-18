# phase.bind.rhythm.bridge
import json
from typing import Dict, Any, Optional
from arch.contract.event.next import next_id, parse_id

# [NEW] Redis 직접 의존성을 제거하고 Tunnel Facade 타입을 가져옵니다.
from arch.topos.bound.tunnel import UniversalFacade

class RhythmBridge:
    """
    @role: 시스템의 심장박동 및 외부 통신 어댑터
    글로벌 Snowflake ID와 인과 Phase ID를 동기화하여 전파합니다.
    """
    def __init__(self, tunnel: UniversalFacade, channel: str):
        # [REFACTORED] redis_async.from_url 대신 주입받은 tunnel을 사용합니다.
        self.tunnel = tunnel
        self.channel = channel

    async def emit(self, psi: Any):
        """이벤트를 Snowflake 및 Phase 정보를 담아 Tunnel(MQ)로 방출"""
        if not getattr(psi, 'event_id', None):
            psi.event_id = next_id()
        
        payload = {
            "event_id": psi.event_id,
            "phase_id": getattr(psi, 'phase_id', 0),
            "kind": getattr(psi, 'kind', 'unknown'),
            "tag": getattr(psi, 'tag', ''),
            "tick": getattr(psi, 'tick', 0),
            "timestamp": parse_id(psi.event_id)['timestamp_ms']
        }
        await self.tunnel.publish(self.channel, json.dumps(payload))