# sphere.ops.launcher
"""
@flow: project -> runtime.detect -> runtime.execute -> deploy.flow
@phase: Ψ  → Φ  → Φx  → residue
@note:
- task unspecified → build → deploy.flow
- task specified   → execute task only
"""
import argparse
import subprocess
import sys
import shutil
from pathlib import Path
from bound.log import get_logger
from bound.resolver import find_current_self
from project.deploy.jar import (
    JarArtifactResolver,
    JarDeployer,
)
from project.deploy.k8s import K8sCI

log = get_logger("ops.launch")

try:
    SELF_ROOT = find_current_self()
except Exception as e:
    log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)

class ProjectRoot:
    """@role: bound.set"""

    def __init__(self, name: str):
        self.path = SELF_ROOT / name
        if not self.path.exists():
            raise FileNotFoundError(
                f"Project directory not found: {self.path}"
            )

class RuntimeDetector:
    """@role: runtime.detector"""

    @staticmethod
    def is_gradle(project_dir: Path) -> bool:
        markers = [
            "build.gradle",
            "build.gradle.kts",
            "settings.gradle",
            "settings.gradle.kts",
            "gradlew",
            "gradlew.bat",
        ]
        return any((project_dir / m).exists() for m in markers)

    @staticmethod
    def is_python(project_dir: Path) -> bool:
        entries = [
            "main.py",
            "app.py",
            "server.py",
            "entrypoint.py",
            "__main__.py",
        ]
        for name in entries:
            if (project_dir / name).exists():
                return True

        markers = [
            "pyproject.toml",
            "setup.py",
            "requirements.txt",
            "Pipfile",
        ]
        return any((project_dir / m).exists() for m in markers)

class RuntimeExecutor:
    """@role: runtime.executor"""

    @staticmethod
    def run_gradle(project_dir: Path, task: str):
        gradlew = project_dir / ("gradlew.bat" if sys.platform == "win32" else "gradlew")
        if gradlew.exists():
            cmd = [str(gradlew), task]
        else:
            if not shutil.which("gradle"):
                raise EnvironmentError(
                    "Gradle CLI not found in PATH"
                )
            cmd = ["gradle", task]

        log.info(f"[exec] {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        assert process.stdout is not None

        for line in process.stdout:
            print(f"[GRADLE] {line}", end="")

        process.wait()
        if process.returncode != 0:
            raise RuntimeError("Gradle execution failed")

    @staticmethod
    def run_python(project_dir: Path):
        candidates = [
            "main.py",
            "app.py",
            "server.py",
            "entrypoint.py",
        ]

        for name in candidates:
            candidate = project_dir / name
            if candidate.exists():
                log.info(f"Detected Python entry: {name}")
                subprocess.run(
                    [sys.executable, str(candidate)],
                    cwd=project_dir,
                    check=True,
                )
                return
        raise FileNotFoundError(
            "Python entrypoint not found"
        )

class DeployFlow:
    """@role: execution trace fixation"""

    @staticmethod
    def has_compose(project_dir: Path):
        return any([
            (project_dir / "docker-compose.yml").exists(),
            (project_dir / "docker-compose.yaml").exists(),
        ])

    @staticmethod
    def deploy_jar(project_dir: Path):
        jar_dir = project_dir / "build" / "libs"
        if not jar_dir.exists():
            raise RuntimeError(
                f"Jar directory not found: {jar_dir}"
            )

        jar_path = JarArtifactResolver.latest(jar_dir)
        log.info(f"Deploying jar: {jar_path}")
        JarDeployer.deploy(
            jar_path,
            resolve_path("lib_jar")
        )

    @staticmethod
    def deploy_k8s(project_dir: Path):
        log.info("Deploying docker-compose → k8s manifests")

        converter = K8sCI(
            docker_base_dir=project_dir
        )

        converter.run()

class LaunchFlow:
    """@role: decision center"""

    @staticmethod
    def run(args):
        project = ProjectRoot(args.dir)
        log.info(f"Project: {project.path}")

        if args.task == "k8s":
            DeployFlow.deploy_k8s(project.path)
            return

        ## Gradle
        if RuntimeDetector.is_gradle(project.path):
            task = args.task or "build"
            log.info("Gradle runtime detected")
            log.info(f"Task: {task}")
            RuntimeExecutor.run_gradle(
                project.path,
                task
            )

            ## default flow: ask unspecified -> build -> deploy.flow
            if args.task is None:

                if DeployFlow.has_compose(project.path):
                    log.info("Running default flow: compose → k8s")
                    DeployFlow.deploy_k8s(project.path)

                else:
                    log.info("Running default flow: build → deploy")
                    DeployFlow.deploy_jar(
                        project.path
                    )

            return

        ## Python
        if RuntimeDetector.is_python(project.path):
            log.info("Python runtime detected")
            RuntimeExecutor.run_python(
                project.path
            )
            return

        raise RuntimeError("Unsupported project runtime")

def main():
    parser = argparse.ArgumentParser(
        description="Project runtime launcher"
    )
    parser.add_argument("--dir", required=True)
    parser.add_argument("--task")
    args = parser.parse_args()
    LaunchFlow.run(args)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"[X] {e}")
        sys.exit(1)