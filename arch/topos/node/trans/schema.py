# arch.topos.node.trans.schema
## @lineage: arch.topos.bound.trans.schema
## @lineage: arch.bound.trans.schema
## @lineage: meta.ator.trans
## @lineage: meta.ator.transductor
## @lineage: meta.flow.transductor
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional, FrozenSet
import libcst as cst
from watcher.plane.emitter import get_emitter

log = get_emitter("ator.trans")

class _SchemaCstAligner(cst.CSTTransformer):
    """클래스 레벨 변환: 이름 변경, 삭제, 필드 타입/기본값 오버라이드, 메서드(Validator) 주입, Model Config 조작 등을 처리"""
    def __init__(
        self, 
        rename_map: Dict[str, str], 
        cull_list: List[str],
        type_overrides: Dict[str, Tuple[str, bool]],
        default_overrides: Dict[str, str],
        validator_injections: Dict[str, str],
        open_classes: FrozenSet[str],
        type_replacements: Dict[str, str]
    ):
        self.rename_map = rename_map or {}
        self.cull_list = cull_list or []
        self.type_overrides = type_overrides or {}
        self.default_overrides = default_overrides or {}
        self.validator_injections = validator_injections or {}
        self.open_classes = open_classes or frozenset()
        self.type_replacements = type_replacements or {}
        self.current_class: Optional[str] = None

    def visit_ClassDef(self, node: cst.ClassDef) -> bool:
        self.current_class = node.name.value
        return True

    def leave_ClassDef(self, original_node: cst.ClassDef, updated_node: cst.ClassDef):
        self.current_class = None
        name = original_node.name.value
        
        ## 불필요한 모델 삭제
        if name in self.cull_list:
            return cst.RemoveFromParent()
            
        ## 클래스명 변경
        new_name = self.rename_map.get(name, name)
        if name in self.rename_map:
            updated_node = updated_node.with_changes(name=cst.Name(new_name))

        new_body = list(updated_node.body.body)

        ## model_config extra="allow" 처리 (MCPS 지원)
        if new_name in self.open_classes:
            for i, stmt in enumerate(new_body):
                if isinstance(stmt, cst.SimpleStatementLine):
                    for obj in stmt.body:
                        if isinstance(obj, cst.Assign) and len(obj.targets) == 1:
                            target = obj.targets[0].target
                            if isinstance(target, cst.Name) and target.value == "model_config":
                                # model_config = ConfigDict(...) 노드를 문자열로 파싱하여 치환 (가장 안전)
                                stmt_code = cst.Module([]).code_for_node(stmt)
                                if 'extra="ignore"' in stmt_code:
                                    stmt_code = stmt_code.replace('extra="ignore"', 'extra="allow"')
                                    new_body[i] = cst.parse_statement(stmt_code)
                                    break
        
        ## 메서드(Validator 등) 주입
        if new_name in self.validator_injections:
            code_to_inject = self.validator_injections[new_name]
            try:
                parsed_method = cst.parse_module(code_to_inject).body[0]
                new_body.append(parsed_method)
            except Exception as e:
                log.error(f"Failed to inject validator into {new_name}: {e}")
                
        return updated_node.with_changes(
            body=updated_node.body.with_changes(body=new_body)
        )

    def leave_AnnAssign(self, original_node: cst.AnnAssign, updated_node: cst.AnnAssign):
        if not self.current_class or not isinstance(original_node.target, cst.Name):
            return updated_node
            
        field_name = original_node.target.value
        field_key = f"{self.current_class}.{field_name}"
        
        ## 텍스트 기반 타입 치환 (MCPS 지원)
        if self.type_replacements:
            ann_code = cst.Module([]).code_for_node(updated_node.annotation.annotation)
            for old, new in self.type_replacements.items():
                if old in ann_code:
                    ann_code = ann_code.replace(old, new)
            try:
                updated_node = updated_node.with_changes(
                    annotation=cst.Annotation(annotation=cst.parse_expression(ann_code))
                )
            except Exception as e:
                 log.error(f"Failed to apply type replacement on {field_key}: {e}")

        ## 필드 타입 오버라이드
        if field_key in self.type_overrides:
            new_type, is_optional = self.type_overrides[field_key]
            type_str = f"Optional[{new_type}]" if is_optional else new_type
            try:
                updated_node = updated_node.with_changes(
                    annotation=cst.Annotation(annotation=cst.parse_expression(type_str))
                )
            except Exception as e:
                log.error(f"Failed to override type for {field_key}: {e}")
            
        ## 기본값 오버라이드
        if field_key in self.default_overrides:
            new_value_str = self.default_overrides[field_key]
            try:
                updated_node = updated_node.with_changes(
                    value=cst.parse_expression(new_value_str)
                )
            except Exception as e:
                log.error(f"Failed to override default for {field_key}: {e}")
            
        return updated_node


class _ModuleLevelAligner(cst.CSTTransformer):
    """모듈 레벨 변환기: 파일 최상단/최하단에 필요한 Import, Enum, 에필로그 등을 주입"""
    def __init__(
        self, 
        enum_injections: List[str], 
        required_imports: List[str], 
        custom_base_model: Optional[str],
        epilogues: List[str]
    ):
        self.enum_injections = enum_injections or []
        self.required_imports = required_imports or []
        self.custom_base_model = custom_base_model
        self.epilogues = epilogues or []

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module):
        new_body = list(updated_node.body)
        
        ## Custom BaseModel 주입
        if self.custom_base_model:
            for i, stmt in enumerate(new_body):
                if isinstance(stmt, cst.SimpleStatementLine):
                    for obj in stmt.body:
                        if isinstance(obj, cst.ImportFrom) and obj.module and obj.module.value == "pydantic":
                            import_code = cst.Module([]).code_for_node(stmt)
                            if "BaseModel" in import_code and "as _BaseModel" not in import_code:
                                import_code = import_code.replace("BaseModel", "BaseModel as _BaseModel")
                                new_body[i] = cst.parse_statement(import_code)
            try:
                base_model_nodes = cst.parse_module(self.custom_base_model).body
                new_body = new_body[:1] + list(base_model_nodes) + new_body[1:]
            except Exception as e:
                log.error(f"Failed to inject custom BaseModel: {e}")

        ## Enum Alias 주입
        enum_nodes = []
        for enum_code in self.enum_injections:
            try:
                enum_nodes.append(cst.parse_statement(enum_code))
            except Exception as e:
                log.error(f"Failed to parse enum injection: {e}")
        
        ## 필수 Import 주입
        import_nodes = []
        for imp in self.required_imports:
            try:
                import_nodes.append(cst.parse_statement(imp))
            except Exception as e:
                log.error(f"Failed to parse import: {e}")
                
        ## 에필로그(최하단) 주입 (MCPS 지원)
        epilogue_nodes = []
        for epi_code in self.epilogues:
            try:
                epilogue_nodes.extend(cst.parse_module(epi_code).body)
            except Exception as e:
                log.error(f"Failed to parse epilogue injection: {e}")

        new_body = import_nodes + enum_nodes + new_body + epilogue_nodes
        return updated_node.with_changes(body=new_body)


class ExternalSchemaTransducer:
    """외부 JSON 스키마를 내부 Pydantic 스키마로 변환하고, 도메인 규칙에 맞게 형태를 다듬는 통합 생성기"""
    def __init__(
        self, 
        base_class_path: str = "pydantic.BaseModel",
        rename_map: Dict[str, str] = None,
        cull_list: List[str] = None,
        type_overrides: Dict[str, Tuple[str, bool]] = None,
        default_overrides: Dict[str, str] = None,
        validator_injections: Dict[str, str] = None,
        enum_injections: List[str] = None,
        required_imports: List[str] = None,
        custom_base_model: str = None,
        open_classes: FrozenSet[str] = None,
        type_replacements: Dict[str, str] = None,
        epilogues: List[str] = None,
        header_comment: str = None,
        extra_args: list[str] = None
    ):
        self.base_class_path = base_class_path
        self.rename_map = rename_map or {}
        self.cull_list = cull_list or []
        self.type_overrides = type_overrides or {}
        self.default_overrides = default_overrides or {}
        self.validator_injections = validator_injections or {}
        self.enum_injections = enum_injections or []
        self.required_imports = required_imports or []
        self.custom_base_model = custom_base_model
        
        self.open_classes = open_classes or frozenset()
        self.type_replacements = type_replacements or {}
        self.epilogues = epilogues or []
        self.header_comment = header_comment
        self.extra_args = extra_args or []

    def transduce(self, input_schema: Path, output_path: Path) -> None:
        log.info(f"  [Transduce] Generating raw schema from {input_schema.name}...")
        
        cmd = [
            sys.executable, "-m", "datamodel_code_generator",
            "--input", str(input_schema),
            "--input-file-type", "jsonschema",
            "--output", str(output_path),
            "--target-python-version", "3.12",
            "--collapse-root-models",
            "--output-model-type", "pydantic_v2.BaseModel",
            "--use-annotated", "--snake-case-field",
            "--base-class", self.base_class_path,
        ] + self.extra_args
        
        subprocess.check_call(cmd)
        log.info(f"  [Transduce] Applying CST structural alignments to {output_path.name}...")
        source_code = output_path.read_text(encoding="utf-8")
        
        ## mcps의 텍스트 치환 중 링크 텍스트 변경 지원 (AST 파싱 전 수행이 더 안전함)
        source_code = source_code.replace("](/", "](https://modelcontextprotocol.io/")

        try:
            tree = cst.parse_module(source_code)
        except Exception as e:
            log.error(f"  [Transduce] Failed to parse generated python file: {e}")
            return
            
        class_aligner = _SchemaCstAligner(
            rename_map=self.rename_map,
            cull_list=self.cull_list,
            type_overrides=self.type_overrides,
            default_overrides=self.default_overrides,
            validator_injections=self.validator_injections,
            open_classes=self.open_classes,
            type_replacements=self.type_replacements
        )
        tree = tree.visit(class_aligner)
        
        module_aligner = _ModuleLevelAligner(
            enum_injections=self.enum_injections,
            required_imports=self.required_imports,
            custom_base_model=self.custom_base_model,
            epilogues=self.epilogues
        )
        tree = tree.visit(module_aligner)
        final_code = tree.code
        ## 헤더 커멘트 주입
        if self.header_comment:
            final_code = self.header_comment + "\n" + final_code

        if source_code != final_code:
            output_path.write_text(final_code, encoding="utf-8")
            log.info("  [Transduce] Schema structural alignment complete.")
        else:
            log.info("  [Transduce] No CST modifications were applied.")