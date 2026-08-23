# watcher.receptor.mem.filter
import json
from typing import Any, Dict, Callable, Set
from dataclasses import dataclass

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mem.filter")

@dataclass(frozen=True)
class EdgeSignature:
    protocol: str          
    client_addr: str
    structural_hash: str   
    intent_tag: str        

class SurvivalAnchor:
    """Dynamic anchor evaluating validity based on real-time kernel topologies."""
    def __init__(self, baseline_efficiency: float = 1.0):
        self.efficiency = baseline_efficiency
        self._valid_topologies: Set[str] = {"core_genesis"} 

    def update_topologies(self, topologies: Set[str]):
        """커널(ReceptorKernel)로부터 현재 유효한 위상 목록을 실시간으로 갱신받음"""
        self._valid_topologies = topologies
        log.info(f"[Membrane] Immune memory updated. Valid topologies: {self._valid_topologies}")

    def evaluate_alignment(self, signature: EdgeSignature) -> bool:
        return (signature.intent_tag in self._valid_topologies or 
                signature.structural_hash in self._valid_topologies)

    def absorb_energy(self):
        self.efficiency += 0.01

class CognitiveMembrane:
    def __init__(self, anchor: SurvivalAnchor):
        self.anchor = anchor

    def validate_signature(self, signature: EdgeSignature) -> bool:
        return self.anchor.evaluate_alignment(signature)

class TunnelL0Interceptor:
    """Raw tunnel filter preventing anomalous byte floods from entering the EventBus."""
    def __init__(self, membrane: CognitiveMembrane):
        self.membrane = membrane

    def intercept(self, channel: str, raw_payload: bytes) -> bool:
        """Returns True if the payload should be dropped."""
        try:
            header_peek = json.loads(raw_payload.decode('utf-8')[:200] + '}') 
            structural_hash = header_peek.get("hash", "")
            intent_tag = header_peek.get("intent", "")
        except Exception:
            structural_hash, intent_tag = "", ""

        signature = EdgeSignature(
            protocol="redis_tunnel",
            client_addr=channel,
            structural_hash=structural_hash,
            intent_tag=intent_tag
        )

        if not self.membrane.validate_signature(signature):
            log.debug(f"[TunnelMembrane] ❌ Unaligned payload on {channel}. Obliterated.")
            return True 

        self.membrane.anchor.absorb_energy()
        return False