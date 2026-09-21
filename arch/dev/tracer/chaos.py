# xphi.arch.dev.tracer.chaos
import os
import sys
import time
import argparse
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass(frozen=True)
class ChaosDef:
    title: str
    code: str
    target_auditor: str
    expected_exit_code: int = 1
    tier: str = "SYSTEM"

class ChaosWorkloads:
    CORE_OOM = ChaosDef(
        title="Resource: Core Daemon OOM (Out of Memory) Injection",
        target_auditor="ContainerStateAuditor (Expects ExitCode 137)",
        expected_exit_code=137,
        code="""
import time, sys
print("[Chaos] Initiating Core Daemon OOM (Out of Memory) Injection...")
sys.stdout.flush()
leaked = []
try:
    while True:
        leaked.append(bytearray(50 * 1024 * 1024))
        print(f"[Chaos] Allocated {len(leaked) * 50 // 1024} MB")
        sys.stdout.flush()
        time.sleep(0.1)
except MemoryError:
    print("[Chaos] Python MemoryError reached (Kube OOMKill expected shortly)")
    sys.exit(137)
        """.strip()
    )

    BUS_DEADLOCK = ChaosDef(
        title="Workload: CPU-bound Deadlock (Event Bus Hang)",
        target_auditor="EntropyAuditor (CPU > 95%) & UniversalLogAuditor",
        expected_exit_code=0,
        code="""
import threading, time, sys
print("[Chaos] Simulating CPU-bound Deadlock (Event Bus Hang)...")
sys.stdout.flush()

def cpu_spinner():
    while True: pass

# 스레드 4개를 돌려 멀티코어 환경에서도 CPU 95% 이상을 유발
for _ in range(4):
    threading.Thread(target=cpu_spinner, daemon=True).start()

time.sleep(1)
# UniversalLogAuditor의 _RULESET_EVENT_BUS 조건을 충족
print("TimeoutError: waiting for PsiEvent")
print("[Chaos] Deadlock logs emitted, spinning CPU indefinitely...")
sys.stdout.flush()

while True:
    time.sleep(1)
        """.strip()
    )

    WORKER_LEAK = ChaosDef(
        title="Resource: Gradual Memory Leak Detection",
        target_auditor="LeakObserverAuditor & UniversalLogAuditor",
        expected_exit_code=1,
        code="""
import time, sys
print("[Chaos] Simulating Background Worker Memory Leak...")
sys.stdout.flush()

registry = {}
counter = 0
while True:
    registry[f"uncollected_task_{counter}"] = bytearray(1024 * 1024)
    
    # UniversalLogAuditor의 _RULESET_CORE_DAEMON 조건을 충족
    if counter % 5 == 0:
        print("[System] Memory growth detected: uncollected objects found.")
        sys.stdout.flush()
        
    counter += 1
    time.sleep(0.5)
        """.strip()
    )

class ChaosRunner:
    def __init__(self, debug: bool = True):
        self.debug = debug
        self.library: Dict[str, ChaosDef] = {
            "oom": ChaosWorkloads.CORE_OOM,
            "deadlock": ChaosWorkloads.BUS_DEADLOCK,
            "leak": ChaosWorkloads.WORKER_LEAK
        }

    def execute(self, scenario_key: str) -> None:
        script = self.library.get(scenario_key)
        if not script:
            print(f"❌ [Chaos Runner] Unknown scenario: '{scenario_key}'")
            sys.exit(1)

        if self.debug:
            print(f"⚠️  [Chaos Runner] Dispatching Scenario: {script.title}")
            print(f"🎯  [Target Auditor]: {script.target_auditor}")
            print("-" * 50)
            
        # 독립적인 실행 컨텍스트(Namespace) 생성
        exec_globals: Dict[str, Any] = {
            "__name__": "__chaos_sandbox__",
            "__doc__": script.title,
        }
        exec_locals: Dict[str, Any] = {}

        start_time = time.time()
        try:
            # 페이로드 코드를 바이트코드로 컴파일 후 실행
            compiled_code = compile(script.code, f"chaos_{scenario_key}.py", 'exec')
            exec(compiled_code, exec_globals, exec_locals)
            
        except KeyboardInterrupt:
            print("\n🛑 [Chaos Runner] Injection aborted by observer.")
            sys.exit(0)
        except SystemExit as e:
            # sys.exit() 호출을 캡처하여 의도된 종료 코드인지 확인
            code = e.code if e.code is not None else 0
            if code == script.expected_exit_code:
                print(f"\n✅ [Chaos Runner] Terminated with expected ExitCode: {code}")
            else:
                print(f"\n⚠️  [Chaos Runner] Terminated with ExitCode: {code} (Expected: {script.expected_exit_code})")
            sys.exit(code)
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            print(f"\n❌ [Chaos Runner] Payload crashed after {elapsed:.2f}ms. Error: {e}")
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="xPhi Data-Driven Chaos Injector")
    parser.add_argument(
        "scenario", 
        choices=["oom", "deadlock", "leak"], 
        help="Target chaos payload to execute"
    )
    parser.add_argument(
        "--quiet", 
        action="store_true", 
        help="Suppress runner metadata logs"
    )
    
    args = parser.parse_args()
    
    runner = ChaosRunner(debug=not args.quiet)
    runner.execute(args.scenario)

if __name__ == "__main__":
    main()