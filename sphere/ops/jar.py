# sphere.ops.jar
"""
@role: artifact.jar.deploy
@flow: project.scan -> build.execute -> artifact.resolve -> artifact.deploy
"""
import subprocess
import sys
import shutil
from pathlib import Path
from bound.log import get_logger
from bound.resolver import (
    find_current_self,
    resolve_path
)

log = get_logger("deploy.jar")

try:
    SELF_ROOT = find_current_self()
    LIB_JAR_ROOT = resolve_path("lib_jar")
except Exception as e:
    log.error(f"[error] 기준면(.self) 탐색 실패: {e}")
    sys.exit(1)

class ProjectScanner:
    """@role: project.scanner"""
    @staticmethod
    def iter_gradle_projects(root: Path):
        for sub in root.iterdir():
            if not sub.is_dir():
                continue

            if (sub / "build.gradle").exists() or (sub / "build.gradle.kts").exists():
                yield sub
            else:
                log.info(f"[skip] not gradle project: {sub.name}")

class JarArtifactResolver:
    """@role: artifact.jar.resolver"""

    @staticmethod
    def latest(jar_dir: Path) -> Path:
        jars = list(jar_dir.glob("*.jar"))
        if not jars:
            raise FileNotFoundError(f"No .jar files found in: {jar_dir}")

        jars.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return jars[0]

class BuildExecutor:
    """@role: build.executor"""

    @staticmethod
    def gradle_build(project_dir: Path):
        cmd = ["./gradlew", "build"]
        log.info(f"[exec] {' '.join(cmd)}")
        subprocess.run(
            cmd,
            cwd=project_dir,
            check=True
        )

class JarDeployer:
    """@role: artifact.jar.deployer"""

    @staticmethod
    def deploy(jar_path: Path, target_root: Path):
        target_root.mkdir(parents=True, exist_ok=True)
        target_path = target_root / jar_path.name
        shutil.copy2(
            jar_path,
            target_path
        )
        log.info(f"[deploy] {jar_path.name} → lib_jar 완료")


class DeployJarPipeline:
    """@role: artifact.deploy.pipeline"""

    def run(self):
        log.info("[AUG] jar_deploy")
        log.info(f"[context] lib_jar = {LIB_JAR_ROOT}")

        for project in ProjectScanner.iter_gradle_projects(SELF_ROOT):
            log.info(f"[build] {project.name} build 시작")

            try:
                BuildExecutor.gradle_build(project)
                jar_path = JarArtifactResolver.latest(
                    project / "build" / "libs"
                )
                JarDeployer.deploy(
                    jar_path,
                    LIB_JAR_ROOT
                )
            except Exception as e:
                log.error(f"[fail] {project.name}: {e}")
                continue

        log.info("[UGA] jar_deploy")


def main():
    DeployJarPipeline().run()

if __name__ == "__main__":
    main()