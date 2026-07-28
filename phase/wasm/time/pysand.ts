// pysand.ts
// @desc: Optimized, Type-Safe, and Memory-Leak-Free Pyodide Sandbox Runner for Deno (with Cgroup features)
import pyodideModule from "npm:pyodide/pyodide.js";
import { readLines } from "https://deno.land/std@0.186.0/io/mod.ts";

// TypeScript Interfaces & Types
interface JsonRpcMessage {
  jsonrpc: "2.0";
  id?: number | string;
}

interface JsonRpcRequest extends JsonRpcMessage {
  method: string;
  params?: Record<string, any>;
}

interface JsonRpcError {
  code: number;
  message: string;
  data?: any;
}

interface ToolParameter {
  name: string;
  type?: string;
  default?: any;
}

// Pyodide's JsProxy type to enforce memory management
interface JsProxy {
  toJs: (options?: any) => any;
  destroy: () => void;
}

// Python Code Templates (Optimized with Virtual Cgroup & Metrics)
const PYTHON_SETUP_CODE = `
import sys, io, json
import tracemalloc

# 메모리 텔레메트리 시작
tracemalloc.start()

old_stdout, old_stderr = sys.stdout, sys.stderr
buf_stdout, buf_stderr = io.StringIO(), io.StringIO()

# 가상 Cgroup 상태 저장소
_cgroup_state = {
    "fuel_quota": None,
    "fuel_consumed": 0,
    "memory_limit_bytes": None
}

def _cgroup_tracer(frame, event, arg):
    """명령어 라인 단위로 가상 Fuel 차감 (무한 루프/과도한 연산 방어)"""
    if _cgroup_state["fuel_quota"] is not None:
        _cgroup_state["fuel_consumed"] += 1
        if _cgroup_state["fuel_consumed"] >= _cgroup_state["fuel_quota"]:
            raise RuntimeError(f"Cgroup Error: CPU Fuel Quota Exceeded ({_cgroup_state['fuel_quota']})")
    return _cgroup_tracer

def _apply_cgroup(fuel, mem_bytes):
    """Control Plane(Broker)에서 전달된 정책을 인-프로세스에 적용"""
    _cgroup_state["fuel_quota"] = fuel
    _cgroup_state["fuel_consumed"] = 0
    _cgroup_state["memory_limit_bytes"] = mem_bytes
    
    if fuel is not None:
        sys.settrace(_cgroup_tracer)
    else:
        sys.settrace(None)

def _get_metrics():
    """현재 샌드박스의 리소스 사용량을 계측하여 반환"""
    current, peak = tracemalloc.get_traced_memory()
    fuel_remaining = -1
    if _cgroup_state["fuel_quota"] is not None:
        fuel_remaining = max(0, _cgroup_state["fuel_quota"] - _cgroup_state["fuel_consumed"])
        
    return json.dumps({
        "mem_usage_bytes": current,
        "mem_peak_bytes": peak,
        "fuel_consumed": _cgroup_state["fuel_consumed"],
        "fuel_remaining": fuel_remaining
    })

def _prepare_execution():
    buf_stdout.seek(0)
    buf_stdout.truncate(0)
    buf_stderr.seek(0)
    buf_stderr.truncate(0)
    sys.stdout, sys.stderr = buf_stdout, buf_stderr

def _restore_execution():
    sys.stdout, sys.stderr = old_stdout, old_stderr
    # 실행 완료 후 안전하게 트레이서 해제
    sys.settrace(None)

def last_exception_args():
    if hasattr(sys, "last_exc") and sys.last_exc:
        return json.dumps(sys.last_exc.args)
    return None

class FinalOutput(BaseException):
    pass

if 'SUBMIT' not in dir():
    def SUBMIT(output):
        raise FinalOutput({"output": output})
`;

const toPythonLiteral = (value: any): string => {
  if (value === null) return 'None';
  if (value === true) return 'True';
  if (value === false) return 'False';
  return JSON.stringify(value);
};

const makeToolWrapper = (toolName: string, parameters: ToolParameter[] = []): string => {
  const sigParts = parameters.map(p => {
    let part = p.name;
    if (p.type) part += `: ${p.type}`;
    if (p.default !== undefined) part += ` = ${toPythonLiteral(p.default)}`;
    return part;
  });
  const kwargParts = parameters.map(p => `"${p.name}": ${p.name}`).join(', ');

  return `
import json
from pyodide.ffi import run_sync, JsProxy
def ${toolName}(${sigParts.join(', ')}):
    result = run_sync(_js_tool_call("${toolName}", json.dumps({"kwargs": {${kwargParts}}})))
    parsed = result.to_py() if isinstance(result, JsProxy) else result
    if isinstance(parsed, dict) and parsed.get("${TOOL_BRIDGE_ERROR_KEY}"):
        raise RuntimeError(parsed.get("message", "Tool bridge error"))
    return parsed
`;
};

const makeSubmitWrapper = (outputs: ToolParameter[]): string => {
  if (!outputs || outputs.length === 0) {
    return `
def SUBMIT(output):
    raise FinalOutput({"output": output})
`;
  }
  const sigParts = outputs.map(o => (o.type ? `${o.name}: ${o.type}` : o.name));
  const dictParts = outputs.map(o => `"${o.name}": ${o.name}`);

  return `
def SUBMIT(${sigParts.join(', ')}):
    raise FinalOutput({${dictParts.join(', ')}})
`;
};

// JSON-RPC 2.0 Helpers
const JSONRPC_PROTOCOL_ERRORS = {
  ParseError: -32700,
  InvalidRequest: -32600,
  MethodNotFound: -32601,
};

const JSONRPC_APP_ERRORS: Record<string, number> = {
  SyntaxError: -32000,
  NameError: -32001,
  TypeError: -32002,
  ValueError: -32003,
  AttributeError: -32004,
  IndexError: -32005,
  KeyError: -32006,
  RuntimeError: -32007,
  CodeInterpreterError: -32008,
  Unknown: -32099,
};

const jsonrpcRequest = (method: string, params: any, id: number | string) =>
  JSON.stringify({ jsonrpc: "2.0", method, params, id });

const jsonrpcResult = (result: any, id: number | string) =>
  JSON.stringify({ jsonrpc: "2.0", result, id });

const jsonrpcError = (code: number, message: string, id: number | string | null, data: any = null) => {
  const err: JsonRpcError = { code, message };
  if (data) err.data = data;
  return JSON.stringify({ jsonrpc: "2.0", error: err, id });
};

// Handle Uncaught Promises gracefully without crashing Deno
globalThis.addEventListener("unhandledrejection", (event) => {
  event.preventDefault();
  console.log(jsonrpcError(JSONRPC_APP_ERRORS.RuntimeError, `Unhandled async error: ${event.reason?.message || event.reason}`, null));
});

// Globals & Initialization
const pyodide = await pyodideModule.loadPyodide();

// [추가] 하드 타임아웃 시 C 레벨에서 파이썬 실행을 멈추기 위한 인터럽트 버퍼
const interruptBuffer = new Uint8Array(new SharedArrayBuffer(1));
pyodide.setInterruptBuffer(interruptBuffer);

const stdinReader = readLines(Deno.stdin);
let requestIdCounter = 0;

const TOOL_BRIDGE_ERROR_KEY = "__meta_tool_bridge_error__";
const createdDirs = new Set<string>(['', '/', '/tmp', '/tmp/spi_vars', '/sandbox']);
for (const dir of createdDirs) {
  try { pyodide.FS.mkdir(dir); } catch (e) { /* exists */ }
}

async function toolCallBridge(name: string, argsJson: string): Promise<any> {
  const requestId = `tc_${Date.now()}_${++requestIdCounter}`;

  try {
    const parsedArgs = JSON.parse(argsJson);
    console.log(jsonrpcRequest("tool_call", { name, kwargs: parsedArgs.kwargs || {} }, requestId));

    const { value: responseLine, done } = await stdinReader.next();
    if (done) throw new Error("stdin closed while waiting for tool response");

    const response = JSON.parse(responseLine);

    if (response.id !== requestId) {
      return { [TOOL_BRIDGE_ERROR_KEY]: true, message: `Tool bridge error: expected id ${requestId}, got ${response.id}` };
    }
    if (response.error) {
      return { [TOOL_BRIDGE_ERROR_KEY]: true, message: `${response.error.data?.type || "ToolError"}: ${response.error.message}` };
    }

    return response.result?.type === "json" ? JSON.parse(response.result.value) : response.result?.value;
  } catch (error: any) {
    return { [TOOL_BRIDGE_ERROR_KEY]: true, message: `Tool bridge error: ${error.message}` };
  }
}

pyodide.globals.set("_js_tool_call", toolCallBridge);

try {
  const env_vars = (Deno.args[0] ?? "").split(",").filter(Boolean);
  for (const key of env_vars) {
    const val = Deno.env.get(key);
    if (val !== undefined) {
      pyodide.runPython(`import os; os.environ[${JSON.stringify(key)}] = ${JSON.stringify(val)}`);
    }
  }
} catch (e) {
  console.error("Error setting environment variables in Pyodide:", e);
}

// 샌드박스 정적 환경 1회 초기화
pyodide.runPython(PYTHON_SETUP_CODE);

// Main Event Loop
while (true) {
  const { value: line, done } = await stdinReader.next();
  if (done) break;

  let input: JsonRpcRequest;
  try {
    input = JSON.parse(line);
  } catch (error: any) {
    console.log(jsonrpcError(JSONRPC_PROTOCOL_ERRORS.ParseError, "Invalid JSON: " + error.message, null));
    continue;
  }

  if (typeof input !== 'object' || input === null || (input as any).jsonrpc !== "2.0") {
    console.log(jsonrpcError(JSONRPC_PROTOCOL_ERRORS.InvalidRequest, "Invalid Request", null));
    continue;
  }

  const { method, params = {}, id: requestId } = input;

  if (method === "sync_file") {
    try {
      const virtualPath = params.virtual_path;
      const hostPath = params.host_path || virtualPath;
      await Deno.writeFile(hostPath, pyodide.FS.readFile(virtualPath));
    } catch (e) { /* ignore */ }
    continue;
  }

  if (method === "shutdown") break;

  if (method === "mount_file") {
    const hostPath = params.host_path;
    const virtualPath = params.virtual_path || hostPath;
    try {
      const contents = await Deno.readFile(hostPath);
      const dirs = virtualPath.split('/').slice(1, -1);
      
      let cur = '';
      for (const d of dirs) {
        cur += '/' + d;
        if (!createdDirs.has(cur)) {
          try { pyodide.FS.mkdir(cur); } catch (e) { /* exists */ }
          createdDirs.add(cur);
        }
      }
      
      pyodide.FS.writeFile(virtualPath, contents);
      if (requestId !== undefined) console.log(jsonrpcResult({ mounted: virtualPath }, requestId));
    } catch (e: any) {
      if (requestId !== undefined) console.log(jsonrpcError(JSONRPC_APP_ERRORS.RuntimeError, `Mount failed: ${e.message}`, requestId));
    }
    continue;
  }

  if (method === "register") {
    const toolNames: string[] = [];
    if (params.tools) {
      for (const tool of params.tools) {
        if (typeof tool === 'string') {
          pyodide.runPython(makeToolWrapper(tool, []));
          toolNames.push(tool);
        } else {
          pyodide.runPython(makeToolWrapper(tool.name, tool.parameters || []));
          toolNames.push(tool.name);
        }
      }
    }
    if (params.outputs) {
      pyodide.runPython(makeSubmitWrapper(params.outputs));
    }
    if (requestId !== undefined) {
      console.log(jsonrpcResult({ tools: toolNames, outputs: params.outputs ? params.outputs.map((o: any) => o.name) : [] }, requestId));
    }
    continue;
  }

  if (method === "inject_var") {
    const { name, value } = params;
    try {
      pyodide.FS.writeFile(`/tmp/spi_vars/${name}.json`, new TextEncoder().encode(value));
      if (requestId !== undefined) console.log(jsonrpcResult({ injected: name }, requestId));
    } catch (e: any) {
      if (requestId !== undefined) console.log(jsonrpcError(JSONRPC_APP_ERRORS.RuntimeError, `Inject failed: ${e.message}`, requestId));
    }
    continue;
  }

  // [추가] Cgroup 정책 주입 RPC 처리
  if (method === "apply_cgroup") {
    const { fuel = null, mem_bytes = null } = params;
    try {
      pyodide.runPython(`_apply_cgroup(${toPythonLiteral(fuel)}, ${toPythonLiteral(mem_bytes)})`);
      if (requestId !== undefined) console.log(jsonrpcResult({ applied: true }, requestId));
    } catch (e: any) {
      if (requestId !== undefined) console.log(jsonrpcError(JSONRPC_APP_ERRORS.RuntimeError, `Cgroup config failed: ${e.message}`, requestId));
    }
    continue;
  }

  // [추가] Metrics 조회 RPC 처리
  if (method === "get_metrics") {
    try {
      const metricsStr = pyodide.runPython("_get_metrics()");
      if (requestId !== undefined) console.log(jsonrpcResult(JSON.parse(metricsStr), requestId));
    } catch (e: any) {
      if (requestId !== undefined) console.log(jsonrpcError(JSONRPC_APP_ERRORS.RuntimeError, `Metrics failed: ${e.message}`, requestId));
    }
    continue;
  }

  if (method === "execute") {
    const code = params.code || "";
    let setupCompleted = false;
    let timeoutId: number | null = null;

    try {
      if (code.includes("import ") || code.includes("from ")) {
        await pyodide.loadPackagesFromImports(code);
      }
      
      pyodide.runPython("_prepare_execution()");
      setupCompleted = true;

      // [추가] Host의 15초 타임아웃 발생 전(12초), 샌드박스 내부에서 안전하게 KeyboardInterrupt(2) 유도
      interruptBuffer[0] = 0;
      timeoutId = setTimeout(() => {
        interruptBuffer[0] = 2; 
      }, 12000);

      const result: JsProxy | null | undefined = await pyodide.runPythonAsync(code);
      let output: any;

      if (result === null || result === undefined) {
        output = pyodide.runPython("buf_stdout.getvalue()");
      } else {
        if (result && typeof result.toJs === 'function') {
          output = result.toJs({ dict_converter: Object.fromEntries });
          result.destroy(); 
        } else {
          output = result;
        }
      }

      if (requestId !== undefined) console.log(jsonrpcResult({ output }, requestId));
    } catch (error: any) {
      const errorType = error.type || "Error";
      const errorMessage = (error.message || "").trim();

      if (errorType === "FinalOutput") {
        const errorArgsStr = pyodide.runPython("last_exception_args()");
        const errorArgs = errorArgsStr ? JSON.parse(errorArgsStr) : [];
        const answer = errorArgs[0] || null;
        if (requestId !== undefined) console.log(jsonrpcResult({ final: answer }, requestId));
      } else {
        let errorArgs: any[] = [];
        if (errorType !== "SyntaxError") {
          const errorArgsStr = pyodide.runPython("last_exception_args()");
          if (errorArgsStr) errorArgs = JSON.parse(errorArgsStr);
        }
        const errorCode = JSONRPC_APP_ERRORS[errorType] || JSONRPC_APP_ERRORS.Unknown;
        if (requestId !== undefined) console.log(jsonrpcError(errorCode, errorMessage, requestId, { type: errorType, args: errorArgs }));
      }
    } finally {
      if (timeoutId !== null) clearTimeout(timeoutId);
      if (setupCompleted) {
        pyodide.runPython("_restore_execution()");
      }
    }
    continue;
  }

  if (requestId !== undefined) {
    console.log(jsonrpcError(JSONRPC_PROTOCOL_ERRORS.MethodNotFound, `Method not found: ${method}`, requestId));
  }
}