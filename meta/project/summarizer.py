# meta.project.summarizer
"""
@desc: Extracts metadata from Markdown and Python files, projecting them into a flat JSON topology.
@flow: Target Directory -> AST/Text Parsing -> State Aggregation -> JSON Dump Emission
"""
import os
import argparse
import ast
import json
from pathlib import Path
from phase.bound.resolver import find_current_self

class ProjectSummarizer:
    """@desc: Repo metadata extractor and structural topology formatter"""
    def __init__(self, target_dir_name: str):
        self.self_root = find_current_self()
        self.target_dir = Path(self.self_root) / target_dir_name

    def extract_md_meta(self, filepath: Path) -> str | None:
        """@desc: Extracts '@xxx' phrases located under the '# title' section in Markdown files"""
        extracted = []
        in_title_section = False
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                ## @step: Read line by line to locate '# title' boundary
                for line in f:
                    line = line.strip()
                    
                    if line.startswith('# '):
                        in_title_section = True
                        continue
                    
                    if in_title_section:
                        ## @point: Extract architectural tags starting with '@'
                        if line.startswith('@'):
                            extracted.append(line)
                        ## @point: Break extraction if a new heading starts or section ends
                        elif line.startswith('#') or (line == "" and len(extracted) > 0):
                            break
        except Exception:
            return None
                            
        return " ".join(extracted).replace('\n', ' ') if extracted else None

    def extract_py_meta(self, filepath: Path) -> str | None:
        """@desc: Extracts the top-level module Docstring or '##' comments in Python files"""
        extracted = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception:
            return None

        ## @step: Extract the top-level module Docstring via AST
        try:
            tree = ast.parse(content)
            docstring = ast.get_docstring(tree)
            if docstring:
                extracted.append(docstring.replace('\n', ' ').strip())
        except SyntaxError:
            pass

        ## @step: Extract detailed '##' comments under the '# title' equivalent section
        lines = content.split('\n')
        in_title_section = False
        
        for line in lines:
            line = line.strip()
            ## @point: Identify the logical title boundary
            if line.startswith('#') and not line.startswith('##') and 'title' in line.lower():
                in_title_section = True
                continue
            
            if in_title_section:
                ## @point: Collect detailed operational tags like @step, @point
                if line.startswith('##'):
                    extracted.append(line.lstrip('#').strip())
                elif line and not line.startswith('#') and not line.startswith('"""') and not line.startswith("'''"):
                    break

        return " | ".join(extracted) if extracted else None

    def execute(self):
        """
        @desc: Executes the directory traversal and outputs the structured metadata JSON
        @flow: Target Directory -> Ast/Text Parsing -> State Aggregation -> JSON Dump Emission
        """
        if not self.target_dir.exists() or not self.target_dir.is_dir():
            # 에러 메시지 역시 후속 LLM 처리를 위해 JSON 포맷으로 방출
            print(json.dumps({"error": f"Directory not found. Path: {self.target_dir}"}))
            return

        ## @step: 상태(메타데이터)와 위상 구조를 담을 공간(Manifold) 생성
        result_map = {}
        
        for filepath in self.target_dir.rglob('*'):
            if not filepath.is_file():
                continue
                
            ## @point: Python과 Markdown 파일만 위상 매핑의 대상으로 한정 (find -name "*.py" 조건과 동일한 커버리지)
            if filepath.suffix not in ['.py', '.md']:
                continue
                
            rel_path = str(filepath.relative_to(self.self_root))
            meta_text = None

            ## @step: Route to specific extractor based on file extension
            if filepath.suffix == '.md':
                meta_text = self.extract_md_meta(filepath)
            elif filepath.suffix == '.py':
                meta_text = self.extract_py_meta(filepath)

            ## @point: 메타데이터가 없더라도 노드의 존재(Topology)를 컨텍스트에 보존
            if not meta_text:
                meta_text = ""

            ## @step: 위상 공간(Map)에 상태 바인딩
            result_map[rel_path] = meta_text

        ## @step: 구조적 결함 없이 안전하게 단일 JSON 객체로 방출 (Emit)
        emitted_json = json.dumps(result_map, indent=2, ensure_ascii=False)
        print(emitted_json)

def main():
    parser = argparse.ArgumentParser(description="Extracts metadata from files and projects them into a flat JSON topology.")
    parser.add_argument('--repo', type=str, required=True, help="Target folder name to scan (relative to SELF_ROOT)")
    args = parser.parse_args()
    
    extractor = ProjectSummarizer(args.repo)
    extractor.execute()

if __name__ == "__main__":
    main()