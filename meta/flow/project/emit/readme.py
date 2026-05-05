# meta.flow.project.emit.readme
from pathlib import Path
from bound.surface.emitter import get_logger
from bound.resolver import resolve_path
from datetime import datetime

BASE_ROOT = resolve_path('base')
log = get_logger("project.readme")

## README 기본 템플릿 정의
README_TEMPLATE = """# {repo_name}

> **Repository Documentation**
> 이 문서는 `{repo_name}` 리포지토리의 모듈 및 컴포넌트 구성을 정의
> *Last Updated: {timestamp}*

## Overview
여기에 `{repo_name}` 리포지토리에 대한 전반적인 설명이나 목적을 기재할 수 있습니다.
(이 영역은 템플릿에서 직접 수정하거나 외부 config에서 주입받을 수 있습니다.)

---

{sections}
"""

class ReadmeGenerator:
    """
    @role: repository markdown aggregator
    @phase: Φ_local → README.md
    """
    def __init__(self, repo_name: str):
        self.target_dir = BASE_ROOT / repo_name
        if not self.target_dir.exists() or not self.target_dir.is_dir():
            raise FileNotFoundError(f"Target repository directory not found: {self.target_dir}")

    def generate(self) -> Path:
        log.info(f"Initiating README generation for repository: {self.target_dir.name}")
        
        # README.md를 제외한 모든 .md 파일 수집 후 이름 순 정렬
        md_files = sorted([
            f for f in self.target_dir.glob("*.md") 
            if f.name.lower() != "readme.md"
        ])
        
        if not md_files:
            log.warning(f"No markdown files found to aggregate in {self.target_dir}")
            return None

        # 각 md 파일을 순회하며 섹션 블록 생성
        section_blocks = []
        for md_file in md_files:
            stem_name = md_file.stem  # 'a.b.md' -> 'a.b'
            content = md_file.read_text(encoding="utf-8").strip()
            
            # 수집된 각 파일을 ## @파일명 섹션으로 구성
            section_blocks.append(f"## @{stem_name}\n\n{content}")

        # 섹션들을 구분선으로 연결
        sections_merged = "\n\n---\n\n".join(section_blocks)

        # 템플릿에 데이터 주입
        final_readme_content = README_TEMPLATE.format(
            repo_name=self.target_dir.name,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sections=sections_merged
        )

        # 최종 파일 쓰기
        output_path = self.target_dir / "README.md"
        output_path.write_text(final_readme_content, encoding="utf-8")
        
        log.info(f"[Φs] README.md successfully generated at {output_path}")
        return output_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Repository README Generator with Template")
    
    parser.add_argument(
        "--dir", 
        required=True, 
        help="Target repository name in 'model' directory to generate README.md"
    )
    
    args = parser.parse_args()

    try:
        generator = ReadmeGenerator(args.dir)
        generator.generate()
    except Exception as e:
        log.error(f"Process failed: {e}")