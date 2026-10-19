# xphi.node.space.sandbox.workspace
import asyncio
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import docker
from docker.errors import BuildError
from docker.models.containers import Container

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

RES_ROOT = resolve_path("res")
SPACE_DIR = RES_ROOT / "space"
CUSTOM_BASE_IMAGE_TAG = "custom-base-image:latest"

DOCKERFILE_CONTENT = """
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git python3-pip
"""


@dataclass
class SandboxWorkspace:
    """ConversationState 및 TerminalTool과의 호환성을 위한 경량 작업 공간 DTO"""
    working_dir: str
    container: Optional[Container] = field(default=None, repr=False)

    def __init__(self, working_dir: str | Path, container: Optional[Container] = None, **_kwargs):
        self.working_dir = str(working_dir)
        self.container = container


class SessionContainerManager:
    """세션별 Docker 컨테이너를 온디맨드로 프로비저닝하고 볼륨 마운트를 관리합니다."""
    
    def __init__(self):
        self.client = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized: 
            return
        
        try:
            self.client = docker.from_env()
            await asyncio.to_thread(self.client.ping)
        except Exception as e:
            log.error("=========================================================")
            log.error(" 🚨 [Error] Failed to connect to the Docker Daemon!")
            log.error(" -> Please verify that Docker Desktop is running.")
            log.error(f" -> Exception details: {str(e)}")
            log.error("=========================================================")
            raise RuntimeError("Docker is not running. Please start Docker Desktop and try again.") from e

        await asyncio.to_thread(SPACE_DIR.mkdir, parents=True, exist_ok=True)
        await self._ensure_image()
        self._initialized = True
        log.info("[SessionContainerManager] Docker environment ready.")

    async def _ensure_image(self) -> None:
        try:
            await asyncio.to_thread(self.client.images.get, CUSTOM_BASE_IMAGE_TAG)
        except docker.errors.ImageNotFound:
            log.info("[SessionContainerManager] Building custom base image...")
            f = io.BytesIO(DOCKERFILE_CONTENT.encode("utf-8"))
            try:
                build_logs = self.client.api.build(fileobj=f, rm=True, tag=CUSTOM_BASE_IMAGE_TAG, decode=True)
                for chunk in build_logs:
                    if "error" in chunk: 
                        raise BuildError(chunk["error"], build_logs)
            except BuildError as e:
                log.error(f"[SessionContainerManager] Image build failed: {e}")
                raise

    async def allocate(self, session_id: str = "shared_workspace", working_dir: str | Path = "/source") -> Container:
        if not self._initialized: 
            await self.initialize()
            
        host_workspace = SPACE_DIR / session_id
        await asyncio.to_thread(host_workspace.mkdir, parents=True, exist_ok=True)

        container_name = f"xphi-space-{session_id}"
        try:
            container = await asyncio.to_thread(self.client.containers.get, container_name)
            if container.status != "running": 
                await asyncio.to_thread(container.start)
            log.info(f"[SessionContainerManager] Reusing existing container: {container_name}")
            return container
        except docker.errors.NotFound:
            pass

        log.info(f"[SessionContainerManager] Spawning new container for session: {session_id}")
        return await asyncio.to_thread(
            self.client.containers.run,
            image=CUSTOM_BASE_IMAGE_TAG,
            name=container_name,
            detach=True,
            tty=True,
            working_dir=str(working_dir),
            command="/bin/bash",
            volumes={
                str(host_workspace.absolute()): {"bind": str(working_dir), "mode": "rw"}
            },
        )

    async def release(self, container: Optional[Container], destroy: bool = False) -> None:
        if not container:
            return
        if destroy:
            log.info(f"[SessionContainerManager] Destroying container {container.name}")
            try:
                await asyncio.to_thread(container.remove, force=True)
            except Exception as e:
                log.warning(f"[SessionContainerManager] Failed to remove container: {e}")
        else:
            log.info(f"[SessionContainerManager] Retaining container {container.name} for future reuse.")