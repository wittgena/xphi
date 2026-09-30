# fiber.infra.config.attestation
import os
from typing import List
from pydantic import BaseModel, Field

class DelegationConfig(BaseModel):
    """Configuration for intent delegation to the Public Core (Origin)."""
    origin_url: str = Field(
        default_factory=lambda: os.getenv("DPHI_ORIGIN_URL", "https://api.fiber.network")
    )
    allow_fallback: bool = Field(
        default_factory=lambda: str(os.getenv("ALLOW_ORIGIN_FALLBACK", "true")).lower() == "true"
    )

class ExportAttestationConfig(BaseModel):
    """Ed25519 Public Keys of Notary nodes required to sign state transitions."""
    @property
    def witness_pubkeys(self) -> List[str]:
        env_validators = os.getenv("COMMITTEE_VALIDATORS")
        if env_validators:
            return [v.strip() for v in env_validators.split(",")]
        return [
            "d9b397e16418eaead7782aaef98dc8b64b550b61c3e1f5f393089da77601a142", 
            "e8c460d3d52c2ab7eb79f42b322a30bb9133a8c66eef4ec3a1d9b3a31c618b7a",
            "1c53e020462002cd43e33d4da3d61ea15a9992d9f4c3bece7d2b2c3a5d848721"
        ]

delegation_config = DelegationConfig()
attestation_config = ExportAttestationConfig()