// pysand.ts
// @desc: Optimized, Type-Safe, and Memory-Leak-Free Pyodide Sandbox Runner for Deno
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

// Python Code Templates (Optimized)
const PYTHON_SETUP_CODE = `
import sys, io, json

old_stdout, old_stderr = sys.stdout, sys.stderr
buf_stdout, buf_stderr = io.StringIO(), io.StringIO()

def _prepare_execution():
    buf_stdout.seek(0)
    buf_stdout.truncate(0)
    buf_stderr.seek(0)
    buf_stderr.truncate(0)
    sys.stdout, sys.stderr = buf_stdout, buf_stderr

def _restore_execution():
    sys.stdout, sys.stderr = old_stdout, old_stderr

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

  if (method === "execute") {
    const code = params.code || "";
    let setupCompleted = false;

    try {
      // [최적화 4] import나 from 키워드가 있을 때만 패키지 로드 검사
      if (code.includes("import ") || code.includes("from ")) {
        await pyodide.loadPackagesFromImports(code);
      }
      
      // [무결성 확보] JsProxy 객체 누수를 차단하기 위해 순수 문자열 기반 runPython 실행
      pyodide.runPython("_prepare_execution()");
      setupCompleted = true;

      const result: JsProxy | null | undefined = await pyodide.runPythonAsync(code);
      let output: any;

      if (result === null || result === undefined) {
        output = pyodide.runPython("buf_stdout.getvalue()");
      } else {
        // [최적화 2] Proxy 객체의 메모리 누수 원천 차단
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
        // [무결성 확보] 에러 처리 중 JsProxy 누수 차단
        const errorArgsStr = pyodide.runPython("last_exception_args()");
        const errorArgs = errorArgsStr ? JSON.parse(errorArgsStr) : [];
        const answer = errorArgs[0] || null;
        if (requestId !== undefined) console.log(jsonrpcResult({ final: answer }, requestId));
      } else {
        let errorArgs: any[] = [];
        if (errorType !== "SyntaxError") {
          // [무결성 확보] 에러 처리 중 JsProxy 누수 차단
          const errorArgsStr = pyodide.runPython("last_exception_args()");
          if (errorArgsStr) errorArgs = JSON.parse(errorArgsStr);
        }
        const errorCode = JSONRPC_APP_ERRORS[errorType] || JSONRPC_APP_ERRORS.Unknown;
        if (requestId !== undefined) console.log(jsonrpcError(errorCode, errorMessage, requestId, { type: errorType, args: errorArgs }));
      }
    } finally {
      if (setupCompleted) {
        // [무결성 확보] 실행 후 환경 원복 시 JsProxy 누수 차단
        pyodide.runPython("_restore_execution()");
      }
    }
    continue;
  }

  if (requestId !== undefined) {
    console.log(jsonrpcError(JSONRPC_PROTOCOL_ERRORS.MethodNotFound, `Method not found: ${method}`, requestId));
  }
}