# topos.arch.code.topic.tracer
## @lineage: arch.model.code.topic.tracer
"""
@flow:
- TopicMap(위상 지도) 기반 목표 모듈 식별
- 동적 임포트(Import) 및 심볼(Callable) 추출
- strike(): Φ -> ∂Φ (reflective injection) -> rupture -> traces
- ExtRegistry에 결과 결속 (Assimilate)
"""
import os
import inspect
import importlib
import sys
import signal
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Any, Dict, List, Optional
from topos.arch.code.topic.registry import TopicRegistry, TopicMap

class TemporalRupture(Exception):
    """@role: 무한 루프나 블로킹 코드를 끊어내는 위상적 시간 한계 예외"""
    pass

@contextmanager
def time_limit(seconds: int):
    """@flow: 시간 제한 장(Field) 형성 -> 한계 도달 시 강제 Rupture 격발"""
    def signal_handler(signum, frame):
        raise TemporalRupture(f"Execution exceeded temporal limit of {seconds}s.")
    
    # 알람 시그널 바인딩 및 타이머 시작
    old_handler = signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        # 정상 종료 시 타이머 해제 및 핸들러 복구
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

class TraceReflector:
    """@flow: access → ∂Φ trace"""
    def __init__(self, trace_log: List[str], path: str = "root"):
        self._trace_log = trace_log
        self._path = path

    def _log_and_rupture(self, action: str):
        current_path = f"{self._path} -> {action}"
        self._trace_log.append(current_path)
        raise RuntimeError(f"round.rupture: {current_path}")

    def __getattr__(self, name): self._log_and_rupture(f"getattr(.{name})")
    def __call__(self, *args, **kwargs): self._log_and_rupture("call()")
    def __getitem__(self, key): self._log_and_rupture(f"getitem([{key}])")
    def __iter__(self): self._log_and_rupture("iter()")

class ExceptionSnapshot:
    """@flow: exception → stack + locals"""
    @staticmethod
    def capture(e: Exception) -> Dict[str, Any]:
        snapshot = {"error": f"{type(e).__name__}: {str(e)}", "stack": []}
        tb = e.__traceback__
        while tb:
            frame = tb.tb_frame
            if "tool/binder" not in frame.f_code.co_filename:
                snapshot["stack"].append({
                    "function": frame.f_code.co_name,
                    "line": tb.tb_lineno,
                    "locals": {k: str(v)[:60] for k, v in frame.f_locals.items() if not k.startswith("__")}
                })
            tb = tb.tb_next
        return snapshot

class TopicTracer:
    """@flow: Scanner -> Tracer -> TopicRegistry"""
    def __init__(self, topic_map: Optional[TopicMap] = None):
        self.topic_map = topic_map
        self.registry = TopicRegistry()  # 상태를 저장할 레지스트리 초기화

    @staticmethod
    def probe(target: Callable) -> Dict[str, Any]:
        """기존의 파괴적 경계면 탐색 로직 (단일 대상)"""
        echoes = {"signature": None, "traces": [], "behavioral_map": {}}
        
        try:
            sig = inspect.signature(target)
            echoes["signature"] = str(sig)
        except (ValueError, TypeError) as e:
            echoes["traces"].append(f"[SigFail] {e}")
            return echoes

        access_log = []
        try:
            args, kwargs = [], {}
            for name, param in sig.parameters.items():
                reflector = TraceReflector(access_log, path=f"param({name})")
                if param.kind == inspect.Parameter.KEYWORD_ONLY:
                    kwargs[name] = reflector
                else:
                    args.append(reflector)
            
            target(*args, **kwargs)
        except Exception as e:
            echoes["traces"].append(f"[exception] {type(e).__name__}")
            echoes["behavioral_map"] = {
                "access_path": access_log,
                "snapshot": ExceptionSnapshot.capture(e)
            }
            
        return echoes

    def run_strategic_scan(self, target_path: Path):
        """TopicMap을 기반으로 코드베이스를 순회하며 반응을 레지스트리에 축적"""
        if not self.topic_map:
            print("[!] TopicMap이 누락되었습니다. 스캔을 건너뜁니다.")
            return

        sys.path.insert(0, str(target_path.absolute()))
        original_argv = sys.argv.copy()
        try:
            for phase_id, phase_space in self.topic_map.spaces.items():
                for core_module in phase_space.core_modules:
                    module_name = core_module.path.replace('\\', '/').replace('/', '.').replace('.py', '')

                    try:
                        sys.argv = [original_argv[0]] 
                        
                        # [개입 지점] 단일 모듈의 로딩 및 프로빙 시간을 3초로 제한
                        with time_limit(3):
                            module = importlib.import_module(module_name)
                            
                            for name, obj in inspect.getmembers(module):
                                if callable(obj) and getattr(obj, '__module__', None) == module_name and not name.startswith('_'):
                                    echoes = self.probe(obj)
                                    self.registry.assimilate(module_name, name, echoes)
                                    
                    except TemporalRupture as e:
                        print(f"[Bounder:Scan] 🚨 [Rupture] Infinite Loop/Blocking Code isolated in '{module_name}'. Moving to next phase.")
                    except SystemExit as e:
                        print(f"[Bounder:Scan] Prevented SystemExit during import ({module_name})")
                    except Exception as e:
                        print(f"[Bounder:Scan] Module Load Failed ({module_name}): {type(e).__name__} - {e}")
                    finally:
                        # 하나의 모듈 로드가 끝날 때마다 상태 복구
                        sys.argv = original_argv
        finally:
            sys.path.pop(0)
            sys.argv = original_argv