# watcher.tracer.bound
## @lineage: ops.watcher.tracer.base
import asyncio
import importlib
import inspect
from abc import ABC, abstractmethod
from functools import wraps
from typing import List, Tuple, Type, Union, Callable, Awaitable, Optional, Any
from watcher.plane.emitter import get_emitter

class BaseBoundary(ABC):
    @abstractmethod
    def collapse(self) -> None:
        pass

class SystemBound(BaseBoundary):
    def __init__(self):
        self.process_pool: List[asyncio.subprocess.Process] = []

    async def run_command(self, cmd: list, cwd: str = ".", capture: bool = False) -> Tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd,
            stdout=asyncio.subprocess.PIPE if capture else None,
            stderr=asyncio.subprocess.PIPE if capture else None
        )
        self.process_pool.append(proc)
        try:
            if capture:
                stdout, stderr = await proc.communicate()
                return proc.returncode, stdout.decode().strip(), stderr.decode().strip()
            
            await proc.wait()
            return proc.returncode, "", ""
        finally:
            if proc in self.process_pool:
                self.process_pool.remove(proc)

    def collapse(self) -> None:
        """Sends a termination signal to all live OS processes."""
        for proc in self.process_pool:
            try:
                if proc.returncode is None:
                    proc.terminate()
            except ProcessLookupError:
                pass
        self.process_pool.clear()

class ExecutorOp:
    """@desc: PhaseOp와 LifecycleOp의 중복 시퀀스 실행 로직을 통합 관리하는 코어 엔진"""
    @staticmethod
    def render_cmd(cmd_template: list, instance) -> list:
        return [arg.format(**instance.__dict__) if isinstance(arg, str) else arg for arg in cmd_template]

    @staticmethod
    async def run_sequence(instance, cmds: List[list], phase_name: str, cwd: str, strict: bool = True) -> bool:
        for i, cmd_template in enumerate(cmds, 1):
            actual_cmd = ExecutorOp.render_cmd(cmd_template, instance)
            instance.log.info(f"## @{phase_name} [{i}/{len(cmds)}]: {' '.join(actual_cmd)}")
            
            code, _, err = await instance.boundary.run_command(actual_cmd, cwd=cwd)
            if strict and code != 0:
                instance.log.crit(f"## @{phase_name}.fault at step {i}: {err}")
                instance.rupture_confirmed = True
                return False
        return True

class PhaseOp:
    @staticmethod
    def stimulus(cmd: list, cwd: str = ".", capture: bool = False, strict: bool = True):
        def decorator(func):
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
                actual_cmd = ExecutorOp.render_cmd(cmd, self)
                self.log.info(f"## @stimulus: {' '.join(actual_cmd)}")
                
                code, out, err = await self.boundary.run_command(actual_cmd, cwd, capture)
                if strict and code != 0:
                    self.log.crit(f"## @stimulus.fault: (Code {code}) {err}")
                    self.rupture_confirmed = True
                    return
                return await func(self, exit_code=code, stdout=out, *args, **kwargs)
            return wrapper
        return decorator

    @staticmethod
    def sequence(cmds: List[list], cwd: str = ".", strict: bool = True):
        def decorator(func):
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
                success = await ExecutorOp.run_sequence(self, cmds, "sequence", cwd, strict)
                if not success:
                    return
                return await func(self, *args, **kwargs)
            return wrapper
        return decorator

class LifecycleOp:
    @staticmethod
    def dynamic_sequence(cmd_provider_attr: str, strict: bool = True):
        def decorator(func):
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
                cmds = getattr(self, cmd_provider_attr)()
                cwd = getattr(self, "workspace", ".")
                success = await ExecutorOp.run_sequence(self, cmds, "lifecycle", cwd, strict)
                if not success:
                    return
                return await func(self, *args, **kwargs)
            return wrapper
        return decorator

class SensorOp:
    @staticmethod
    def poll(cmd: list):
        def decorator(func):
            func.__sensor_cmd__ = cmd
            return func
        return decorator

def log_streamer(cmd_list: list):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            cmd = [arg.format(**self.__dict__) if isinstance(arg, str) else arg for arg in cmd_list]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.STDOUT
            )
            if hasattr(self, 'boundary') and hasattr(self.boundary, 'process_pool'):
                self.boundary.process_pool.append(proc)
            try:
                while True:
                    line = await proc.stdout.readline()
                    if not line: break
                    await func(self, line.decode().strip(), *args, **kwargs)
            except asyncio.CancelledError:
                pass
            finally:
                if proc.returncode is None:
                    proc.terminate()
        return wrapper
    return decorator

class BaseAuditor(ABC):
    def __init__(self, target: str, namespace: str, boundary: Union[BaseBoundary, Any]):
        self.target = target
        self.namespace = namespace
        self.boundary = boundary
        self._task = None

    def attach(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._observe())

    def detach(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    async def _observe(self) -> None:
        sensors = []
        for name in dir(self):
            method = getattr(self, name)
            if hasattr(method, '__sensor_cmd__'):
                sensors.append(method)

        try:
            while True:
                for sensor in sensors:
                    cmd = [arg.format(**self.__dict__) for arg in sensor.__sensor_cmd__]
                    if hasattr(self.boundary, 'run_command'):
                        code, out, _ = await self.boundary.run_command(cmd, capture=True)
                        if code == 0 and out:
                            sensor(out)
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

class BaseStreamAuditor(ABC):
    """@desc: Abstract base class for continuous streaming sensors."""
    def __init__(self, target: str, boundary: Union[BaseBoundary, Any], delay: int = 1):
        self.target = target
        self.boundary = boundary
        self.delay = delay
        self._task = None

    def attach(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._start_stream())

    def detach(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    async def _start_stream(self) -> None:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        await self.run_stream()

    @abstractmethod
    async def run_stream(self, *args, **kwargs) -> None:
        pass

class BaseTracer(ABC):
    """@desc: Universal Abstract Base Class for system divergence tracing."""
    def __init__(self, tracer_name: str, timeout: int = 60, boundary: Optional[BaseBoundary] = None):
        self.log = get_emitter(f"tracer.{tracer_name}", phase="agent")
        self.timeout = timeout
        self.boundary = boundary or SystemBound()
        self.tasks: List[asyncio.Task] = []
        self.auditors: List[Union[BaseAuditor, BaseStreamAuditor]] = []
        self.rupture_confirmed: bool = False
        self.equilibrium_achieved: bool = False

    @classmethod
    async def select(cls, module_path: str, **kwargs) -> "BaseTracer":
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(f"[Framework] Failed to load tracer module '{module_path}': {e}")

        tracer_cls: Type['BaseTracer'] = None
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, cls) and obj is not cls and not inspect.isabstract(obj):
                tracer_cls = obj
                break
        
        if not tracer_cls:
            raise ValueError(f"[Framework] No valid BaseTracer found in '{module_path}'")

        instance = tracer_cls(**kwargs)
        await instance.trace()
        return instance

    def register_auditors(self, *auditors: Union[BaseAuditor, BaseStreamAuditor]) -> None:
        """@desc: Auditor들을 등록하고 즉시 라이프사이클(attach)에 편입시킵니다."""
        for auditor in auditors:
            self.auditors.append(auditor)
            auditor.attach()

    async def await_rupture(self, tick: int = 1, hook_fn: Optional[Callable[[int], Awaitable[None]]] = None) -> None:
        """@desc: 타임아웃까지 파괴(Rupture) 상태를 폴링합니다. hook_fn을 통해 매 틱마다 로직을 주입할 수 있습니다."""
        remaining = self.timeout
        while remaining > 0 and not self.rupture_confirmed:
            if hook_fn:
                await hook_fn(remaining)
            
            if remaining % 5 == 0 and remaining != self.timeout:
                self.log.info(f"  -> Resonance Window: {remaining}s remaining...")
                
            await asyncio.sleep(tick)
            remaining -= tick

    @abstractmethod
    async def execute(self) -> None:
        pass

    async def trace(self) -> None:
        self.log.crit(f"## @trace.init: Initiating Structural Trace Lifecycle")
        try:
            await asyncio.wait_for(self.execute(), timeout=self.timeout)
        except asyncio.TimeoutError:
            self.log.warning(f"## @trace.timeout: Trace execution reached time limit ({self.timeout}s).")
        except asyncio.CancelledError:
            self.log.warning("## @trace.cancel: Trace execution was cancelled externally.")
            raise
        except Exception as e:
            self.log.crit(f"## @trace.error: Framework Fault Detected: {str(e)}")
        finally:
            self.log.info("## @trace.teardown: Collapsing space and reclaiming resources...")
            for auditor in self.auditors:
                auditor.detach()
            
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            self.boundary.collapse()
            self.log.crit("## @trace.exit: Field Restored.")

class ReproBaseTracer(BaseTracer):
    """@desc: Specialized BaseTracer utilizing LifecycleOp for declarative infrastructure control"""
    def __init__(self, target_name: str, timeout: int = 60, boundary: Optional[BaseBoundary] = None):
        super().__init__(tracer_name=f"repro.{target_name}", timeout=timeout, boundary=boundary)
        # self.config = TargetRegistry.get(target_name)
        # self.workspace = self.config["workspace_path"]