# contract.proposer
import ast
import re
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Set
from collections import defaultdict
from bound.resolver import find_current_self, resolve_path
from anchor.around import discover_repos
from flow.emitter import get_emitter
from contract.registry import contract

SELF_ROOT = find_current_self()
CONTRACT_ROOT = resolve_path('contract')
log = get_emitter('contract.proposer')

## Dynamic Registry Introspection
VALID_CONTRACT_ATTRS: Set[str] = set(vars(contract).keys())
VALID_CONTRACT_FUNCS: Set[str] = {
    func.__name__ for func in vars(contract).values() if callable(func)
}
VALID_CONTRACT_FUNCS.add("bootstrap_contract")

class UnifiedToposVisitor(ast.NodeVisitor):
    """명시적 데코레이터와 암묵적 @flow 주석을 추출"""
    def __init__(self, module_fqn: str):
        self.module_fqn = module_fqn
        self.found_nodes: List[Dict[str, Any]] = []

    def _is_contract_decorator(self, name: str) -> bool:
        if not name: return False
        if name.startswith('contract.'):
            return name.split('.')[1] in VALID_CONTRACT_ATTRS
        return name in VALID_CONTRACT_FUNCS

    def _extract_args(self, call_node: ast.Call) -> Dict[str, Any]:
        extracted = {"_positional": [], "_kwargs": {}}
        for arg in call_node.args:
            if isinstance(arg, ast.Constant):
                extracted["_positional"].append(arg.value)
        for kw in call_node.keywords:
            if kw.arg:
                if isinstance(kw.value, ast.Constant):
                    extracted["_kwargs"][kw.arg] = kw.value.value
                elif isinstance(kw.value, ast.List):
                    extracted["_kwargs"][kw.arg] = [
                        elt.value for elt in kw.value.elts if isinstance(elt, ast.Constant)
                    ]
        return extracted

    def _get_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name): return node.id
        elif isinstance(node, ast.Attribute):
            val = self._get_name(node.value)
            return f"{val}.{node.attr}" if val else ""
        return ""

    def _extract_function_hints(self, node: ast.FunctionDef) -> Dict[str, Any]:
        args = [arg.arg for arg in node.args.args if arg.arg not in ('self', 'cls')]
        return {"args": args}

    def _parse_flow_docstring(self, docstring: str) -> Dict[str, Any]:
        """@flow: Docstring에서 @flow 패턴을 search -> Requires/Emits를 자동 추론"""
        if not docstring: return {}
        match = re.search(r'@flow\s+(.+)', docstring)
        if not match: return {}
        
        evidence = match.group(1).strip()
        
        # [FIX] 화살표(-> 또는 →)가 존재하지 않는 단순 설명문은 무시합니다.
        if '->' not in evidence and '→' not in evidence:
            return {}
            
        parts = re.split(r'\s*(?:->|→)\s*', evidence)
        
        if len(parts) >= 2:
            return {
                "requires": [parts[0].strip()],
                "emits": [parts[-1].strip()],
                "evidence": evidence
            }
        return {}

    def _analyze_node(self, node: ast.AST, node_type: str, hints: Dict[str, Any]):
        """데코레이터와 주석을 종합적으로 평가하여 노드를 구성"""
        ## 암묵적 계약 (Docstring 파싱)
        docstring = ast.get_docstring(node)
        implicit_contract = self._parse_flow_docstring(docstring)
        
        ## 명시적 계약 (Decorator 파싱)
        explicit_contracts = []
        decorator_types = []
        positional_args = []
        
        for decorator in node.decorator_list:
            dec_name = ""
            args = {"_positional": [], "_kwargs": {}}
            
            if isinstance(decorator, ast.Call):
                dec_name = self._get_name(decorator.func)
                args = self._extract_args(decorator)
            elif isinstance(decorator, (ast.Name, ast.Attribute)):
                dec_name = self._get_name(decorator)
            
            if self._is_contract_decorator(dec_name):
                explicit_contracts.append(args["_kwargs"])
                decorator_types.append(dec_name)
                positional_args.extend(args["_positional"])

        ## 유효한 노드 식별 및 역할(Role) 부여
        if explicit_contracts or implicit_contract:
            if explicit_contracts and implicit_contract:
                role = "Hybrid Node"
            elif explicit_contracts:
                role = "Explicit Node"
            else:
                role = "Implicit Node"

            dec_type = decorator_types[0] if decorator_types else "implicit.flow"
            explicit_base = explicit_contracts[0] if explicit_contracts else {}

            self.found_nodes.append({
                "fqn": f"{self.module_fqn}.{node.name}",
                "decorator_type": dec_type,
                "target_type": node_type,
                "role": role,
                "contract": {
                    "explicit": explicit_base,
                    "implicit": implicit_contract,
                    "inferred": {}
                },
                "shape_hints": hints or {},
                "positional_args": positional_args
            })

    def visit_FunctionDef(self, node: ast.FunctionDef):
        hints = {"signature": self._extract_function_hints(node)}
        self._analyze_node(node, "function", hints)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):
        methods_hints = {}
        for n in node.body:
            if isinstance(n, ast.FunctionDef) and not n.name.startswith("__"):
                methods_hints[n.name] = self._extract_function_hints(n)
                
        hints = {"methods": list(methods_hints.keys()), "signatures": methods_hints}
        self._analyze_node(node, "class", hints)
        self.generic_visit(node)


def generate_task_proposals(repos: List[Path]) -> Dict[str, List[Dict[str, Any]]]:
    proposals = defaultdict(list)
    start_time = time.time()
    scanned_files = 0
    
    for repo in repos:
        if not repo.exists() or not repo.is_dir():
            continue

        for py_file in repo.rglob("*.py"):
            if py_file.name.startswith("_") and py_file.name != "__init__.py":
                continue

            try:
                relative = py_file.relative_to(repo)
                module_fqn = ".".join(relative.with_suffix("").parts)
                
                source = py_file.read_text(encoding="utf-8")
                tree = ast.parse(source, filename=str(py_file))
                
                visitor = UnifiedToposVisitor(module_fqn)
                visitor.visit(tree)
                
                for node_data in visitor.found_nodes:
                    dec_type = node_data.pop("decorator_type")
                    role = node_data.get("role")
                    
                    ## 그룹화(Grouping) 전략
                    if 'bootstrap' in dec_type:
                        group = node_data["positional_args"][0] if node_data["positional_args"] else "ungrouped"
                    elif role == "Implicit Node":
                        ## Implicit 노드는 fqn의 두 번째 파트를 그룹으로 사용 (예: meta.bound.xxx -> bound)
                        parts = node_data['fqn'].split('.')
                        group = parts[1] if len(parts) > 1 else "implicit"
                    else:
                        group = "phase.nodes"
                        
                    proposals[group].append(node_data)
                    
                scanned_files += 1
            except Exception:
                pass 

    elapsed = time.time() - start_time
    log.info(f"[AST Generator] Scanned {scanned_files} files across {len(repos)} repos in {elapsed:.3f}s")
    return dict(proposals)


def print_console_summary(proposals: Dict[str, List[Dict[str, Any]]]):
    print("\n## Topos Proposal (Unified State)")
    total_nodes = 0
    
    for group, nodes in proposals.items():
        print(f"\n> Group: {group} (Count: {len(nodes)})")
        sorted_nodes = sorted(nodes, key=lambda x: (x.get('target_type', ''), x['fqn']))
        
        for i, node in enumerate(sorted_nodes):
            is_last = (i == len(sorted_nodes) - 1)
            prefix = "  └──" if is_last else "  ├──"
            
            fqn = node['fqn']
            target_type = node.get('target_type', 'unknown')
            role = node.get('role', 'Unknown')
            
            contract_data = node.get('contract', {})
            explicit = contract_data.get('explicit', {})
            implicit = contract_data.get('implicit', {})
            
            ## 출력할 파이프라인 식별 (우선순위: Explicit > Implicit)
            req = ",".join(explicit.get('requires', [])) if explicit.get('requires') else ",".join(implicit.get('requires', []))
            emi = ",".join(explicit.get('emits', [])) if explicit.get('emits') else ",".join(implicit.get('emits', []))
            
            ## Role에 따른 시각화 태그 설정
            if role == "Implicit Node":
                tag = "[Flow]"
            elif role == "Hybrid Node":
                tag = "[Hybr]"
            elif not req and not emi:
                tag = "[Fluid]"
            else:
                tag = "[Cryst]"
                
            req_str = req or '~'
            emi_str = emi or '~'
            
            print(f"{prefix} [{target_type[:4].upper()}] {fqn} ({req_str} ➔ {emi_str}) {tag}")
            
        total_nodes += len(nodes)

    print(f"## Total Unified Nodes: {total_nodes}\n")

if __name__ == "__main__":
    SELF_ROOT = find_current_self()
    target_repos = discover_repos(SELF_ROOT)
    
    if not target_repos:
        log.warning("No repositories discovered under SELF_ROOT.")
        exit(1)
        
    tasks = generate_task_proposals(target_repos)
    print_console_summary(tasks)
    
    ## 디렉토리 존재 확인 및 생성
    CONTRACT_ROOT.mkdir(parents=True, exist_ok=True)
    proposal_path = CONTRACT_ROOT / "tasks.json"
    
    with open(proposal_path, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, ensure_ascii=False)
        
    log.info(f"Successfully generated unified proposal map at: {proposal_path}")