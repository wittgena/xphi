# arch.model.payload
## @lineage: arch.bound.payload
## @lineage: arch.topos.bound.payload
import json
import time
import logging
from enum import Enum
from datetime import datetime, date
from uuid import UUID
from typing import Any, Dict

log = logging.getLogger("bound.payload")

class SafeJSONEncoder(json.JSONEncoder):
    """
    @desc: A custom JSON encoder for safely serializing complex Python objects
           (Enum, datetime, UUID, Pydantic models) that the default encoder cannot handle.
    """
    def default(self, obj):
        # 1. Enum: Extract raw value
        if isinstance(obj, Enum):
            return obj.value
        
        # 2. Datetime/Date: Convert to ISO 8601 string
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
            
        # 3. UUID: Convert to string
        if isinstance(obj, UUID):
            return str(obj)
            
        # 4. Pydantic Models: Attempt fallback to dict hydration
        if hasattr(obj, 'model_dump'):
            return obj.model_dump(mode="json")
            
        return super().default(obj)


class StreamPayloadAdapter:
    """
    @desc: Communication adapter overcoming Redis Stream's flat mapping constraints.
           Now includes Zero-Intrusion capabilities to transparently inject and unwrap
           WASM telemetry (Topos/Phase/Nexus Parity) without breaking the monolith.
    """
    DATA_KEY = "data"
    DATA_KEY_BYTES = b"data"

    @classmethod
    def encode(cls, payload: Any) -> Dict[str, str]:
        """
        Compresses a complex payload into a JSON string for Redis Stream.
        If `_telemetry` is detected, it generates the Cryptographic Parity Triplet
        and wraps the payload in a WASM-compatible Envelope.
        """
        final_data = payload

        # Check for telemetry metadata injected by Agent/Gov
        if isinstance(payload, dict) and "_telemetry" in payload:
            # Create a shallow copy to prevent mutating the original business object
            final_data = dict(payload)
            telemetry = final_data.pop("_telemetry")
            
            try:
                # Late import to prevent circular dependencies
                from xphi.arch.contract.event.next import generate_parity_triplet
                
                # 1. Generate Parity Triplet using telemetry metrics
                parity = generate_parity_triplet(
                    topo=telemetry.get("topo", 0),
                    press=telemetry.get("press", 0),
                    rupture=telemetry.get("rupture", False)
                )
                
                # 2. Construct WASM Envelope (PhaseDrift compatible)
                final_data = {
                    "_wasm_envelope": True,
                    "context": {
                        "timestamp": int(time.time() * 1000),
                        "injected_anchor": parity["nexus_id"],
                        "injected_tick": telemetry.get("tick", 0)
                    },
                    "parity": parity,
                    "data": final_data  # Pure business payload
                }
                
            except ImportError:
                log.warning("[PayloadAdapter] arch.contract.event.next not found. Telemetry ignored.")
            except Exception as e:
                log.warning(f"[PayloadAdapter] Failed to envelop telemetry: {e}")

        return {cls.DATA_KEY: json.dumps(final_data, cls=SafeJSONEncoder)}

    @classmethod
    def decode(cls, stream_message: Dict[Any, Any]) -> Any:
        """
        Extracts and restores Python objects from Redis Stream messages.
        Transparently strips the WASM Envelope if present, ensuring the
        main Conver loop receives only the expected business payload.
        """
        raw_data = stream_message.get(cls.DATA_KEY) or stream_message.get(cls.DATA_KEY_BYTES)
        
        if not raw_data:
            return stream_message  # Fallback for standard flat dicts
            
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
            
        try:
            parsed_data = json.loads(raw_data)
            if isinstance(parsed_data, dict) and parsed_data.get("_wasm_envelope"):
                return parsed_data.get("data", parsed_data)
            return parsed_data
            
        except json.JSONDecodeError:
            return raw_data  # Fallback for plain strings