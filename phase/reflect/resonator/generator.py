# phase.reflect.resonator.generator
import asyncio
import logging
import inspect
from typing import Any, Dict, List, Optional
from pathlib import Path
from arch.proto.flow import ProtoFlow, FlowState, Transduction, Align
from topos.plane.emitter import get_logger
from dataclasses import dataclass, field
from arch.bound.resolver import find_current_self
from arch.contract.registry import registry, contract
from arch.bound.ator.runtime import AtorRuntime
from arch.bound.ator.bootstrap import bootstrap
from phase.node.runtime import NodeRuntime

log = logging.getLogger("resonance.generator")

@dataclass
class CodeBlock:
    block_type: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class NormalizedOp:
    name: str
    op_type: str
    effect: str
    inputs: List[str]
    outputs: List[str]
    raw_block: CodeBlock

class BlockExtractor:
    """테스트를 위해 가짜 Python 블록을 반환하는 Mock Extractor"""
    def extract(self, file_path: str) -> List[CodeBlock]:
        log.info(f"  [Extractor] Extracting fragments from {file_path}")
        dummy_code = "def sample_function():\n    with open('test.txt', 'r') as f:\n        return f.read()"
        return [CodeBlock(block_type="python", content=dummy_code)]

class BlockNormalizer:
    def normalize(self, blocks: List[CodeBlock]) -> List[NormalizedOp]:
        ops = []
        for block in blocks:
            if block.block_type == "python":
                ops.append(self._normalize_python(block))
        return [op for op in ops if op is not None]

    def _normalize_python(self, block: CodeBlock) -> Optional[NormalizedOp]:
        code = block.content
        if "open(" in code or "Path(" in code:
            return NormalizedOp(
                name=f"file_io_op",
                op_type="transduction",
                effect="filesystem",
                inputs=["file_path"],
                outputs=["file_content"],
                raw_block=block
            )
        return None

class ScriptSynthesizer:
    def synthesize(self, ops: List[NormalizedOp]) -> Dict[str, Any]:
        script_topology = {}
        for i, op in enumerate(ops):
            node_id = f"step_{i:02d}_{op.name}"
            script_topology[node_id] = {
                "type": "ator" if op.op_type == "transduction" else "resonance",
                "spec": {
                    "role": f"auto_{op.name}",
                    "next": "UGA",
                    "operator": op.name
                }
            }
        return script_topology

class CodeGenerator:
    def generate_code(self, op: NormalizedOp) -> str:
        class_name = op.name.replace("_", " ").title().replace(" ", "")
        code = f"""
@contract.ator("{op.name}")
class {class_name}(Transduction):
    def _execute_transformation(self, data, instruction):
        # [Auto-Extracted Code]
{self._indent_code(op.raw_block.content, indent=8)}
        return data
"""
        return code.strip()

    def _indent_code(self, code: str, indent: int) -> str:
        spaces = " " * indent
        return "\n".join(spaces + line for line in code.split("\n"))

class MetaTranscriptor:
    def __init__(self):
        self.extractor = BlockExtractor()   
        self.normalizer = BlockNormalizer()
        self.synthesizer = ScriptSynthesizer()
        self.generator = CodeGenerator()

    def ingest(self, file_path: str):
        blocks = self.extractor.extract(file_path)
        ops = self.normalizer.normalize(blocks)
        script_topology = self.synthesizer.synthesize(ops)
        ator_codes = {op.name: self.generator.generate_code(op) for op in ops}
        return script_topology, ator_codes

@contract.ator("ator.generator")
class AtorGenerator(Transduction):
    def transduce(self, flow: ProtoFlow, ator_node: Any) -> ProtoFlow:
        log.info(f"  [Generator] Initiating Meta-Transcription Process...")
        
        target_file = flow.payload.get("target_file", "./dummy_source.py")
        transcriptor = MetaTranscriptor()
        
        ## 전체 파이프라인 실행
        script_topology, ator_codes = transcriptor.ingest(find_current_self() / target_file)
        
        ## 파일에 쓰기 좋게 문자열로 결합 (Aligner로 전달할 페이로드 구성)
        output_code = f"import json\n\n# --- GENERATED SCRIPT TOPOS ---\nAUG = {script_topology}\n\n# --- GENERATED ATORS ---\n"
        for name, code in ator_codes.items():
            output_code += f"\n{code}\n"
            
        log.info("  [Transcriptor] Transcription successful. Handing over to Aligner.")
        return ProtoFlow(
            payload={"code": output_code},
            aspect="transcribed",
            id=flow.id
        )

def materialize_topology(topology: Dict[str, Any]) -> Dict[str, Any]:
    runtime_nodes = {}
    for node_id, config in topology.items():
        node_type = config["type"]
        spec = config["spec"]
        
        NodeClass = registry._nodes[node_type].node_class
        node_instance = NodeClass(spec)
        
        target_operator = spec.get("operator")
        if target_operator:
            operator_instance = registry.create_component("ator", {"type": target_operator})
            node_instance.bound_operator = operator_instance
            
        runtime_nodes[node_id] = node_instance
    return runtime_nodes

async def main():
    log.info(">>> Launching Ator Generation Pipeline <<<")
    current_script_path = inspect.getsourcefile(lambda: None)
    base_node, flow_controller, entry_id = await bootstrap(current_script_path)

    try:
        # payload에 파싱할 타겟 파일 경로를 넘겨줌
        initial_payload = {
            "task_id": "GEN-001",
            "requirement": "Extract and generate Ators",
            "target_file": "./some_legacy_script.py" 
        }
        initial_ctx = FlowState(ProtoFlow(payload=initial_payload, aspect="init"), state={})
        
        log.info(f"Submitting task to entry node [{entry_id}]...")
        await base_node.psi_queue.put((entry_id, initial_ctx))
        await base_node.psi_queue.join()
        
        log.info(">>> Field Stabilized: Execution Complete.")
    except Exception as e:
        log.error(f"Execution Error: {e}", exc_info=True)
    finally:
        base_node.running = False

if __name__ == "__main__":
    asyncio.run(main())