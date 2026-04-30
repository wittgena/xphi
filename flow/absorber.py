# flow.absorber
"""
@flow: Ψ_total (files) -> sample (Anchor selection based on Dest size) → extract (AST imports) → resonance (similarity) → bind (absorb) → re-topologize
"""
import ast
import random
import shutil
import argparse
from pathlib import Path
from typing import Set, Dict

class ProjectAbsorber:
    """Φ_inducer: induce structure from Ψ via local anchors"""
    def __init__(self, source_dir: str, dest_dir: str, ratio: float = 0.1, similarity_threshold: float = 0.3):
        self.source_dir = Path(source_dir).resolve()
        self.dest_dir = Path(dest_dir).resolve()
        self.ratio = ratio
        self.threshold = similarity_threshold
        self.import_cache: Dict[Path, Set[str]] = {}

    def extract_imports(self, path: Path) -> Set[str]:
        """Ψ(file) → Ψ_feature(import projection) with Noise Cancellation"""
        if path in self.import_cache:
            return self.import_cache[path]

        imports = set()
        NOISE = {
            "os", "sys", "json", "ast", "pathlib", "typing", "collections", 
            "argparse", "logging", "re", "datetime", "math", "time", "random", 
            "shutil", "itertools", "functools", "copy", "uuid", "abc", "io"
        }

        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        base_mod = alias.name.split('.')[0]
                        if base_mod not in NOISE:
                            imports.add(alias.name) 
                            
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0:
                        parent_dir = path.parent.name
                        if node.module:
                            imports.add(f"{parent_dir}.{node.module}")
                    elif node.module:
                        base_mod = node.module.split('.')[0]
                        if base_mod not in NOISE:
                            imports.add(node.module)
        except Exception:
            pass

        self.import_cache[path] = imports
        return imports

    def calculate_similarity(self, path_a: Path, path_b: Path) -> float:
        """Ψ_a ⊕ Ψ_b → θ (resonance score)"""
        imports_a = self.extract_imports(path_a)
        imports_b = self.extract_imports(path_b)
        
        if not imports_a or not imports_b:
            return 0.0

        intersection = len(imports_a & imports_b)
        union = len(imports_a | imports_b)
        if intersection == 0:
            return 0.0
            
        return intersection / union if union > 0 else 0.0

    def run(self, apply: bool = False):
        """@flow: Ψ_total → Φ_seed → Φ_cluster → Φ'"""
        if not self.source_dir.exists():
            print(f"[Error] Source directory not found: {self.source_dir}")
            return

        all_src_files = list(self.source_dir.rglob("*.py"))
        if not all_src_files:
            print("[Error] No python files found in source.")
            return

        all_dest_files = list(self.dest_dir.rglob("*.py"))
        dest_count = len(all_dest_files)

        if dest_count == 0:
            print(f"[Warning] '{self.dest_dir.name}' is empty. Defaulting to 1 anchor.")
            num_anchors = 1
        else:
            num_anchors = max(1, int(dest_count * self.ratio))

        num_anchors = min(num_anchors, len(all_src_files))
        anchors = random.sample(all_src_files, num_anchors)
        remaining_files = set(all_src_files) - set(anchors)

        print(f"## Semantic Project Cluster (Mode: {'APPLY' if apply else 'DRY-RUN'})")
        print("="*65)
        print(f"Source                   : {len(all_src_files)} files")
        print(f"Dest                     : {dest_count} files")
        print(f"Anchors Selected         : {num_anchors} (Based on {self.ratio*100}% of Dest files)")
        print(f"Similarity Threshold     : {self.threshold * 100}%\n")

        ## Anchor processing
        for anchor in anchors:
            """@flow: Φ_seed(local) → induce Φ_cluster"""
            rel_path = anchor.relative_to(self.source_dir)
            anchor_dest = self.dest_dir / rel_path
            anchor_cluster_dir = anchor_dest.parent
            print(f"\n[Anchor] 📍 {rel_path}")

            if apply:
                anchor_cluster_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(anchor, anchor_dest)
            else:
                print(f"  └─ (COPY) -> {anchor_dest}")

            ## Resonance + Absorption
            absorbed_files = []
            for candidate in list(remaining_files):
                similarity = self.calculate_similarity(anchor, candidate)

                if similarity >= self.threshold:
                    candidate_dest = anchor_cluster_dir / candidate.name
                    if apply:
                        if candidate_dest.exists():
                            candidate_dest = anchor_cluster_dir / f"{candidate.stem}_absorbed.py"
                        
                        shutil.copy2(candidate, candidate_dest)
                        bak_path = candidate.with_name(candidate.name + ".bak")
                        shutil.move(str(candidate), str(bak_path))
                    else:
                        print(f"  └─ [Match: {similarity:.2f}] {candidate.relative_to(self.source_dir)}")
                        print(f"       └─ (MOVE) -> {candidate_dest}")
                    
                    absorbed_files.append(candidate)
                    remaining_files.remove(candidate)
            if apply and absorbed_files:
                print(f"  └─ Absorbed {len(absorbed_files)} similar modules into {anchor_cluster_dir.name}/")

        if apply:
            print(f"- Clustering Complete! Files absorbed into {self.dest_dir}")
        else:
            print("- DRY-RUN Complete. Add '--apply' to execute the physical clustering.")


def main():
    parser = argparse.ArgumentParser(description="Dest-scaled Anchor Selection & Semantic Clustering")
    parser.add_argument("--src", required=True, help="Source directory (Huge pool, e.g., dissolve_target)")
    parser.add_argument("--dest", required=True, help="Destination directory (Base structure, e.g., meta)")
    parser.add_argument("--ratio", type=float, default=0.1, help="Ratio based on DEST file count (default: 0.1)")
    parser.add_argument("--threshold", type=float, default=0.3, help="Jaccard similarity threshold (0.0 ~ 1.0, default: 0.3)")
    parser.add_argument("--apply", action="store_true", help="Execute physical copy and move")

    args = parser.parse_args()
    absorber = ProjectAbsorber(
        source_dir=args.src,
        dest_dir=args.dest,
        ratio=args.ratio,
        similarity_threshold=args.threshold
    )
    absorber.run(apply=args.apply)

if __name__ == "__main__":
    main()