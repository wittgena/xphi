# watcher.surface.topos
import time
import redis

class RedisTopos:
    """
    @role: ∂Φ boundary surface (Substrate)
    - stores current Φ
    - resonance Ψ / Ψ′ via pubsub
    """
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.state_key = "meta.self:state:current_phase"
        self.signal_channel = "meta.self:signals:phase_mutation"
        self.psi_channel = "meta.self:signals:psi"

    def get_current_phase(self) -> str:
        """Φ read (default = Φ0)"""
        return self.client.get(self.state_key) or "Φ0"

    def set_phase(self, phase: str):
        """Φ write (State Mutation)"""
        self.client.set(self.state_key, phase)

    def emit_psi(self, event_type: str, weight: int = 1):
        """
        @desc: Ψ emission
        @flow: Ψ → surface → (Prometheus / external Φ)
        """
        payload = {"event": event_type, "weight": weight, "ts": time.time()}
        print(f"Ψ emit → {payload}")
        self.client.publish(self.psi_channel, str(payload))