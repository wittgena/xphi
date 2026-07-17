# arch.topos.surge.blueprint
## @lineage: arch.topos.state.surge.blueprint
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class SurgeNode(BaseModel):
    """@desc: A single discrete cognitive or operational step within the execution manifold"""
    id: str = Field(..., description="Unique topological identifier for this node (e.g., 'step_1_rupture_analysis').")
    intent: str = Field(..., description="The cognitive intent (e.g., 'explore', 'modify', 'evangelize', 'commit').")
    action: str = Field(..., description="Target tool name matching the CoreTool registry (e.g., 'terminal', 'signal', 'finish').")
    description: str = Field(..., description="Detailed instruction and scope for this specific step.")
    expected_outcome: str = Field(..., description="The validation criteria to determine if this node has converged successfully.")
    params_template: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="Pre-configured arguments to constrain or guide the tool execution (e.g., predefined Slack channels)."
    )

class SurgeBlueprint(BaseModel):
    """@desc: The overarching Directed Acyclic Graph (DAG) specification for an Agent's cognitive trajectory"""
    topology_name: str = Field(..., description="Human-readable name of this structural flow.")
    focus: str = Field(..., description="The primary objective or cognitive state validation goal.")
    depth_limit: int = Field(default=4, description="Maximum execution depth or iterative limit before forcing a halt.")
    relations_constraint: str = Field(default="sequential", description="How nodes relate to each other (e.g., 'sequential', 'coupled', 'isolated').")
    system_instructions: str = Field(..., description="The foundational LLM system prompt dictating the rules of engagement for this specific topology.")
    nodes: List[SurgeNode] = Field(..., description="The ordered sequence or graph of cognitive nodes to traverse.")

    @classmethod
    def from_dict(cls, raw_blueprint: Dict[str, Any]) -> "SurgeBlueprint":
        """Dictionary(예: RESOLUTION_BLUEPRINT)를 안전한 내부 타입으로 변환하는 파서"""
        return cls.model_validate(raw_blueprint)