# meta.project.summarizer
"""
@desc: Extracts metadata from Markdown and Python files, formatting them into a single-line JSON structure for optimal LLM context injection.
@phase: meta.project.emit (Context Bundling & Alignment)
@flow: Target Directory -> AST/Text Parsing -> Metadata Extraction -> JSON String Emission
"""
import os
import argparse
import ast
from pathlib import Path
from phase.bound.resolver import find_current_self

class ProjectSummarizer:
    """@desc: Repo metadata extractor and single-line JSON formatter"""
    def __init__(self, target_dir_name: str):
        self.self_root = find_current_self()
        self.target_dir = Path(self.self_root) / target_dir_name

    def extract_md_meta(self, filepath: Path) -> str | None:
        """@desc: Extracts '@xxx' phrases located under the '# title' section in Markdown files"""
        extracted = []
        in_title_section = False
        
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
                        
        return " ".join(extracted).replace('\n', ' ') if extracted else None

    def extract_py_meta(self, filepath: Path) -> str | None:
        """@desc: Extracts the top-level module Docstring or '##' comments in Python files"""
        extracted = []
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

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
        """@desc: Executes the directory traversal and outputs the formatted metadata"""
        if not self.target_dir.exists() or not self.target_dir.is_dir():
            print(f"Error: Directory not found. Path: {self.target_dir}")
            return

        ## @step: Output line-by-line in a dictionary-like format for strict LLM consumption
        print("{")
        
        is_first = True
        for filepath in self.target_dir.rglob('*'):
            if not filepath.is_file():
                continue
                
            rel_path = str(filepath.relative_to(self.self_root))
            meta_text = None

            ## @step: Route to specific extractor based on file extension
            if filepath.suffix == '.md':
                meta_text = self.extract_md_meta(filepath)
            elif filepath.suffix == '.py':
                meta_text = self.extract_py_meta(filepath)

            if meta_text:
                if not is_first:
                    print(",")
                ## @point: Escape double quotes and compress to a single line
                safe_value = meta_text.replace('"', '\\"')
                print(f'  "{rel_path}": "{safe_value}"', end="")
                is_first = False
                
        print("\n}")

def main():
    parser = argparse.ArgumentParser(description="Extracts and compresses metadata from files in a directory into a single-line format.")
    parser.add_argument('--repo', type=str, required=True, help="Target folder name to scan (relative to SELF_ROOT)")
    args = parser.parse_args()
    extractor = ProjectSummarizer(args.repo)
    extractor.execute()

if __name__ == "__main__":
    main()