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
    // [Security] Prevent Out-of-Bounds (Infinite Loop) due to missing Null Terminator
    while (endPtr < memView.length && memView[endPtr] !== 0) {
        endPtr++;
    }
    return new TextDecoder().decode(memView.subarray(ptr, endPtr));
}

async function executePureWasm(vmTarget: string, payload: any): Promise {
    console.log(`[WASM] ⚙️ Initializing WASM module for target: ${vmTarget}`); 
    let targetModule = dvmModule;
    if (vmTarget === "DPHI") targetModule = dphiModule;
    if (vmTarget === "COSMWASM_EXTERNAL") targetModule = cw20Module;

    const instance = await WebAssembly.instantiate(targetModule, { env: { invoke_native_vm: () => 0 } });
    const exports: any = instance.exports;
    const memory = exports.memory as WebAssembly.Memory;

    // [WASM ABI] Serialize JSON payload to string
    // 여기서 인자로 받은 payload 객체가 문자열로 직렬화됩니다.
    const payloadStr = JSON.stringify(payload || {});
    
    // [ABI Format] DVM requires Null-Terminator, DPHI relies on explicit length
    const payloadBytes = new TextEncoder().encode(vmTarget === "DPHI" ? payloadStr : payloadStr + '\0');
    
    console.log(`[WASM] 📦 Payload size: ${payloadBytes.length} bytes`); 
    
    const codePtr = exports.alloc(payloadBytes.length);
    console.log(`[WASM] 💾 Allocated memory ptr: ${codePtr}`); 
    new Uint8Array(memory.buffer).set(payloadBytes, codePtr);

    try {
        if (vmTarget === "DPHI") {
            // =========================================================
            // [DPHI ABI] invoke_wasm(ptr: u32, len: u32) -> u64 (ptr | len)
            // =========================================================
            if (typeof exports.invoke_wasm !== "function") {
                throw new Error("WASM Trap: dphi.wasm does not export 'invoke_wasm'");
            }
            
            console.log(`[WASM] 🚀 Calling invoke_wasm(ptr: ${codePtr}, len:${payloadBytes.length})`); 
            const t0 = Date.now(); 
            
            // 1. Invoke WASM function (Returns 64-bit integer)
            const resPacked = BigInt(exports.invoke_wasm(codePtr, payloadBytes.length));
            console.log(`[WASM] ⏱️ invoke_wasm returned in ${Date.now() - t0}ms. Packed res:${resPacked}`); 
            
            // 2. Decode Upper 32-bit (Pointer) and Lower 32-bit (Length)
            const resPtr = Number(resPacked >> 32n);
            const resLen = Number(resPacked & 0xFFFFFFFFn);
            console.log(`[WASM] 🔍 Decoded resPtr: ${resPtr}, resLen:${resLen}`); 
            
            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from DPHI");

            // 3. Read exact length from memory
            const memView = new Uint8Array(memory.buffer);
            const resultStr = new TextDecoder().decode(memView.subarray(resPtr, resPtr + resLen));
            
            // 4. Deallocate memory
            if (exports.dealloc) {
                console.log(`[WASM] 🧹 Deallocating output memory (ptr: ${resPtr}, len:${resLen})`); 
                exports.dealloc(resPtr, resLen);
            }
            return resultStr;

        } else {
            // =========================================================
            // [DVM / COSMWASM ABI] execute_router(ptr: u32) -> u32 (null-terminated)
            // =========================================================
            if (typeof exports.execute_router !== "function") {
                throw new Error(`WASM Trap: ${vmTarget} does not export 'execute_router'`);
            }

            console.log(`[WASM] 🚀 Calling execute_router(ptr: ${codePtr})`); 
            const t0 = Date.now(); 
            const resPtr = exports.execute_router(codePtr);
            console.log(`[WASM] ⏱️ execute_router returned in ${Date.now() - t0}ms. resPtr:${resPtr}`); 
            
            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from DVM");

            const resultStr = readCString(memory, resPtr);
            return resultStr;
        }
    } finally {
        // [Security] Reclaim input payload memory under all circumstances
        if (exports.dealloc) {
            console.log(`[WASM] 🧹 Deallocating input memory (ptr: ${codePtr}, len:${payloadBytes.length})`); 
            exports.dealloc(codePtr, payloadBytes.length);
        }
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise {
        const fetchStart = Date.now(); 
        if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

        try {
            const input: any = await request.json();
            const reqId = input.id || "unknown"; 
            const vmTarget = (input.params?.vm_target || "PYTHON").toUpperCase();
            
            console.log(`[Router] 📥 Received Request ID: ${reqId} | Target:${vmTarget}`); 

            // 1. Handle WASM natively
            if (vmTarget === "DVM" || vmTarget === "DPHI" || vmTarget === "COSMWASM_EXTERNAL") {
                let resultStr = "";
                
                try {
                    // [Core] Pass-through payload without modification (Already formatted by tunnel.flare)
                    resultStr = await executePureWasm(vmTarget, input.params?.payload);
                } catch (err: any) {
                    console.error(`[Router] 🚨 WASM Trap/Panic caught for ID: ${reqId}`, err.message); 
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
                    console.log(`[Router] ⚠️ Empty WASM result for ID: ${reqId}`); 
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
                            console.log(`[Router] ⚠️ WASM returned logical error for ID: ${reqId} | Msg:${errorMessage}`);
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
                            console.log(`[Router] ⚠️ WASM plain text error matched for ID: ${reqId}`);
                        } else {
                            isSuccess = true;
                        }
                    }
                }

                const headers = { "Content-Type": "application/json" };
                console.log(`[Router] 📤 Sending WASM Response for ID: ${reqId} | Success:${isSuccess} | Elapsed: ${Date.now() - fetchStart}ms`);
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

            // =========================================================================
            // 2. Transformed: Proxy to Python Engine + WASM Hashing Coordinator
            // =========================================================================
            if (input.method === "execute" || vmTarget === "PYTHON" || input.params?.method_func === "execute_code") {
                console.log(`[Router] 🐍 Proxying Request ID: ${reqId} to PYTHON_ENGINE...`);
                const tProxyStart = Date.now();
                
                const proxyRequest = new Request("http://internal-python/execute", {
                    method: "POST",
                    body: JSON.stringify(input),
                    headers: { "Content-Type": "application/json" }
                });
                
                // 1) Python Engine으로 실행 요청 및 응답 가로채기
                const pythonResponse = await env.PYTHON_ENGINE.fetch(proxyRequest);
                console.log(`[Router] ✅ PYTHON_ENGINE responded for ID: ${reqId} in${Date.now() - tProxyStart}ms | HTTP ${pythonResponse.status}`);
                
                const responseText = await pythonResponse.text();
                let pyParsed: any = {};
                try {
                    pyParsed = JSON.parse(responseText);
                } catch (e) {
                    console.error(`[Router] ⚠️ Failed to parse Python response for ID: ${reqId}`);
                }

                // 2) 성공(result) 또는 에러(error) Payload에서 Metrics 데이터 추출
                const action = input.params?.method_func || input.method || "execute_code";
                const metrics = pyParsed.result?.metrics || pyParsed.error?.data?.metrics || {
                    fuel_consumed: 1, mem_usage_bytes: 1024, tier: "EDGE_UNKNOWN"
                };

                // 3) dphi.wasm의 compute_root_fingerprint 메서드를 위한 페이로드 포맷팅
                const canonicalRecord = {
                    action: action,
                    tier: metrics.tier,
                    fuel: metrics.fuel_consumed,
                    mem_usage: metrics.mem_usage_bytes
                };
                
                const hashPayload = {
                    method: "compute_root_fingerprint", // [수정됨] Rust의 #[serde(rename_all = "snake_case")]에 맞게 정확한 소문자 매핑
                    context: { timestamp: Date.now() },
                    payload: [canonicalRecord]          // [수정됨] 이중 직렬화(JSON.stringify) 제거. 순수 배열로 전달.
                };

                // 4) DPHI WASM 호출하여 Canonical Hash 씰링(Sealing)
                let executionHash = "HASH_GENERATION_FAILED";
                try {
                    console.log(`[Router] 🔐 Sealing Execution Hash at Edge for ID: ${reqId}...`);
                    const wasmResStr = await executePureWasm("DPHI", hashPayload);
                    const wasmRes = JSON.parse(wasmResStr);
                    
                    if (wasmRes.success && wasmRes.data) {
                        // WASM의 직렬화 방식에 따라 이중 파싱이 필요할 수 있음
                        executionHash = typeof wasmRes.data === 'string' 
                            ? JSON.parse(wasmRes.data).fingerprint 
                            : wasmRes.data.fingerprint;
                        console.log(`[Router] 🔗 Canonical Hash Sealed: ${executionHash}`);
                    } else {
                        // [추가됨] 실패 시 구체적인 에러 사유를 로그로 출력 (디버깅 용이성 확보)
                        console.error(`[Router] ⚠️ WASM Hash Generation Rejected. Error: ${wasmRes.error}`);
                    }
                } catch (e) {
                    console.error(`[Router] 🚨 Hash computation failed for ID: ${reqId}`, e);
                }

                // 5) 원래의 Python 응답을 유지하면서, 생성된 Hash를 Header에 주입하여 최종 반환
                const finalHeaders = new Headers(pythonResponse.headers);
                finalHeaders.set("X-XPHI-Canonical-Hash", executionHash);

                return new Response(responseText, {
                    status: pythonResponse.status,
                    headers: finalHeaders
                });
            }

            console.warn(`[Router] ❌ Unsupported method/target for ID: ${reqId} | Target:${vmTarget}`);
            return new Response(JSON.stringify({ 
                jsonrpc: "2.0", 
                error: { message: `Method or Target not supported: ${vmTarget}` },
                id: input.id 
            }), { status: 400, headers: { "Content-Type": "application/json" } });

        } catch (e: any) {
            console.error(`[Router] 🚨 Internal Router Fault:`, e);
            return new Response(JSON.stringify({ 
                jsonrpc: "2.0",
                error: { message: `Router Internal Error: ${e.message}` } 
            }), { status: 500, headers: { "Content-Type": "application/json" } });
        }
    }
};