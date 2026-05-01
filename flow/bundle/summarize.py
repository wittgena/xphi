# flow.bundle.summarize
import os
import argparse
import ast
from pathlib import Path
from bound.resolver import find_current_self

class ProjectSummarize:
    """Repo 내의 마크다운 및 파이썬 파일에서 메타데이터를 추출하여 LLM 주입에 최적화된 단일 라인 형태로 포맷팅"""
    def __init__(self, target_dir_name: str):
        self.self_root = find_current_self()
        self.target_dir = Path(self.self_root) / target_dir_name

    def extract_md_meta(self, filepath: Path) -> str | None:
        """Markdown 파일에서 '# title' 아래의 '@xxx' 구문 추출"""
        extracted = []
        in_title_section = False
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                if line.startswith('# '):
                    in_title_section = True
                    continue
                
                if in_title_section:
                    if line.startswith('@'):
                        extracted.append(line)
                    elif line.startswith('#') or (line == "" and len(extracted) > 0):
                        break
                        
        return " ".join(extracted).replace('\n', ' ') if extracted else None

    def extract_py_meta(self, filepath: Path) -> str | None:
        """Python 파일에서 최상단 Docstring 또는 '##' 주석 추출"""
        extracted = []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # 1. 최상단 모듈 Docstring 추출
        try:
            tree = ast.parse(content)
            docstring = ast.get_docstring(tree)
            if docstring:
                extracted.append(docstring.replace('\n', ' ').strip())
        except SyntaxError:
            pass

        # 2. '# title' 하위의 '##' 주석 추출
        lines = content.split('\n')
        in_title_section = False
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('#') and not line.startswith('##') and 'title' in line.lower():
                in_title_section = True
                continue
                
            if in_title_section:
                if line.startswith('##'):
                    extracted.append(line.lstrip('#').strip())
                elif line and not line.startswith('#') and not line.startswith('"""') and not line.startswith("'''"):
                    break

        return " | ".join(extracted) if extracted else None

    def execute(self):
        """탐색 및 출력 실행"""
        if not self.target_dir.exists() or not self.target_dir.is_dir():
            print(f"Error: 디렉토리를 찾을 수 없습니다. 경로: {self.target_dir}")
            return

        # LLM이 읽기 편하도록 딕셔너리 형태의 라인 단위 출력
        print("{")
        
        is_first = True
        for filepath in self.target_dir.rglob('*'):
            if not filepath.is_file():
                continue
                
            rel_path = str(filepath.relative_to(self.self_root))
            meta_text = None

            if filepath.suffix == '.md':
                meta_text = self.extract_md_meta(filepath)
            elif filepath.suffix == '.py':
                meta_text = self.extract_py_meta(filepath)

            if meta_text:
                if not is_first:
                    print(",")
                # 따옴표 이스케이프 처리 후 1줄로 출력
                safe_value = meta_text.replace('"', '\\"')
                print(f'  "{rel_path}": "{safe_value}"', end="")
                is_first = False
                
        print("\n}")


def main():
    parser = argparse.ArgumentParser(description="디렉토리 내 파일들의 메타데이터를 추출하여 1줄 단위로 압축합니다.")
    parser.add_argument('--repo', type=str, required=True, help="탐색할 대상 폴더명 (SELF_ROOT 하위 기준)")
    args = parser.parse_args()

    extractor = ProjectSummarize(args.repo)
    extractor.execute()

if __name__ == "__main__":
    main()