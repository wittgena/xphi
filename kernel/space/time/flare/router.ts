// kernel.space.time.flare.router.ts
import dvmModule from "./dvm.wasm";
import dphiModule from "./dphi.wasm";
import cw20Module from "./cw20_base.wasm";

interface Env {
    PYTHON_ENGINE: Fetcher; // Service Binding to Python Worker
}

function readCString(memory: WebAssembly.Memory, ptr: number): string {
    const memView = new Uint8Array(memory.buffer);
    let endPtr = ptr;
    // [보안 패치] Null Terminator 누락으로 인한 Out of Bounds(무한 루프) 방지
    while (endPtr < memView.length && memView[endPtr] !== 0) {
        endPtr++;
    }
    return new TextDecoder().decode(memView.subarray(ptr, endPtr));
}

async function executePureWasm(vmTarget: string, payload: any): Promise<string> {
    let targetModule = dvmModule;
    if (vmTarget === "DPHI") targetModule = dphiModule;
    if (vmTarget === "COSMWASM_EXTERNAL") targetModule = cw20Module;

    const instance = await WebAssembly.instantiate(targetModule, { env: { invoke_native_vm: () => 0 } });
    const exports: any = instance.exports;
    const memory = exports.memory as WebAssembly.Memory;

    // [WASM ABI 정렬] tunnel.flare에서 조립한 완벽한 규격의 JSON 객체(payload)를 그대로 문자열로 직렬화
    const payloadStr = JSON.stringify(payload || {});
    // DVM은 Null-Terminator(\0)가 필수지만, DPHI는 길이를 명시하므로 불필요함. 
    const payloadBytes = new TextEncoder().encode(vmTarget === "DPHI" ? payloadStr : payloadStr + '\0');
    
    const codePtr = exports.alloc(payloadBytes.length);
    new Uint8Array(memory.buffer).set(payloadBytes, codePtr);

    // [보안 패치] Memory Leak 방지용 try-finally 블록
    try {
        if (vmTarget === "DPHI") {
            // =========================================================
            // [DPHI ABI] invoke_wasm(ptr: u32, len: u32) -> u64 (ptr | len)
            // =========================================================
            if (typeof exports.invoke_wasm !== "function") {
                throw new Error("WASM Trap: dphi.wasm does not export 'invoke_wasm'");
            }
            
            // 1. WASM 함수 호출 (64비트 정수로 반환됨)
            const resPacked = BigInt(exports.invoke_wasm(codePtr, payloadBytes.length));
            
            // 2. 상위 32비트(포인터), 하위 32비트(길이) 분리 디코딩
            const resPtr = Number(resPacked >> 32n);
            const resLen = Number(resPacked & 0xFFFFFFFFn);
            
            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from DPHI");

            // 3. 메모리에서 정확한 길이만큼만 읽기
            const memView = new Uint8Array(memory.buffer);
            const resultStr = new TextDecoder().decode(memView.subarray(resPtr, resPtr + resLen));
            
            // 4. 결과값 메모리 반환
            if (exports.dealloc) exports.dealloc(resPtr, resLen);
            return resultStr;

        } else {
            // =========================================================
            // [DVM / COSMWASM ABI] execute_router(ptr: u32) -> u32 (null-terminated)
            // =========================================================
            if (typeof exports.execute_router !== "function") {
                throw new Error(`WASM Trap: ${vmTarget} does not export 'execute_router'`);
            }

            const resPtr = exports.execute_router(codePtr);
            
            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from DVM");

            const resultStr = readCString(memory, resPtr);
            return resultStr;
        }
    } finally {
        // 어떠한 경우에도 파이썬이 할당했던 입력 페이로드 메모리를 완벽히 회수
        if (exports.dealloc) exports.dealloc(codePtr, payloadBytes.length);
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

        try {
            const input: any = await request.json();
            const vmTarget = (input.params?.vm_target || "PYTHON").toUpperCase();

            // 1. WASM 직접 처리 로직
            if (vmTarget === "DVM" || vmTarget === "DPHI" || vmTarget === "COSMWASM_EXTERNAL") {
                let resultStr = "";
                
                try {
                    // [핵심 패치] tunnel.flare가 이미 Rust 스키마에 맞는 {method, context, payload} 
                    // 형태의 봉투를 input.params.payload 에 담아서 보냈으므로, 어떠한 조작도 없이 순수 패스스루 시킵니다.
                    resultStr = await executePureWasm(vmTarget, input.params?.payload);
                } catch (err: any) {
                    return new Response(JSON.stringify({ 
                        jsonrpc: "2.0", 
                        error: { message: `WASM Trap/Panic: ${err.message}` }, 
                        id: input.id 
                    }), { headers: { "Content-Type": "application/json" }});
                }

                let isSuccess = false;
                let errorMessage = "WASM Logic Execution Failed";
                let wasmParsed = null;

                if (!resultStr || resultStr.trim() === "") {
                    isSuccess = false;
                    errorMessage = "WASM Execution blocked: Unregistered API or Silent Trap";
                } else {
                    try {
                        wasmParsed = JSON.parse(resultStr);
                        if (wasmParsed.success === false || wasmParsed.error !== undefined) {
                            isSuccess = false;
                            const rawErr = wasmParsed.error;
                            if (typeof rawErr === 'object' && rawErr?.message) {
                                errorMessage = rawErr.message;
                            } else {
                                errorMessage = rawErr || wasmParsed.revert_reason || "WASM Logic Execution Failed";
                            }
                        } else {
                            isSuccess = true;
                        }
                    } catch (e) {
                        const lowerRes = resultStr.toLowerCase();
                        if (lowerRes.includes("error") || lowerRes.includes("failed") || 
                            lowerRes.includes("denied") || lowerRes.includes("trap") || 
                            lowerRes.includes("panic")) {
                            isSuccess = false;
                            errorMessage = `WASM Execution Error: ${resultStr}`;
                        } else {
                            isSuccess = true;
                        }
                    }
                }

                const headers = { "Content-Type": "application/json" };
                
                if (isSuccess) {
                    return new Response(JSON.stringify({ 
                        jsonrpc: "2.0", 
                        result: { output: resultStr }, 
                        id: input.id 
                    }), { headers });
                } else {
                    return new Response(JSON.stringify({ 
                        jsonrpc: "2.0", 
                        error: { message: errorMessage, data: wasmParsed }, 
                        id: input.id 
                    }), { headers });
                }
            }

            // 2. Python 엔진으로 투명하게 프록싱
            if (input.method === "execute" || vmTarget === "PYTHON" || input.params?.method_func === "execute_code") {
                const proxyRequest = new Request("http://internal-python/execute", {
                    method: "POST",
                    body: JSON.stringify(input),
                    headers: { "Content-Type": "application/json" }
                });
                return await env.PYTHON_ENGINE.fetch(proxyRequest);
            }

            return new Response(JSON.stringify({ 
                jsonrpc: "2.0", 
                error: { message: `Method or Target not supported: ${vmTarget}` },
                id: input.id 
            }), { status: 400, headers: { "Content-Type": "application/json" } });

        } catch (e: any) {
            return new Response(JSON.stringify({ 
                jsonrpc: "2.0",
                error: { message: `Router Internal Error: ${e.message}` } 
            }), { status: 500, headers: { "Content-Type": "application/json" } });
        }
    }
};