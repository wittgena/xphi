# arch.code.frag.datetime
## @lineage: xor.block.frag.datetime
from pydantic import Field
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID
from phase.bind.event.next import next_id

def utc_now() -> datetime:
    """Return the current time in UTC (``datetime.utcnow`` is deprecated)."""
    return datetime.now(UTC)


def _uuid_to_hex(uuid_obj: UUID) -> str:
    return uuid_obj.hex

ToposId = Annotated[
    str, 
    Field(
        default_factory=next_id, 
        description="Topological Snowflake ID containing Manifold & Vertex context"
    )
]