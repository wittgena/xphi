// xphi.node.space.time.flare.router.ts
import phaseModule from "./phase.wasm";
import fsmModule from "./fsm.wasm";

export interface Env {
    PYTHON_ENGINE: Fetcher;      // Service Binding to Python Worker
    XPHI_EDGE_TOKEN?: string;    // Mode 2: Private BYOC pre-shared secret
    XPHI_X402_RECEIVER?: string; // Mode 3: Public x402 settlement address/policy
}

const EdgeHeader = {
    CANONICAL_HASH: "X-XPHI-Canonical-Hash",
    STATE: "X-Edge-State",
    EDGE_TOKEN: "X-Fiber-Edge-Token",
    FUEL_RECEIPT: "X-Fiber-Fuel-Receipt",
} as const;

function verifyEdgeIngress(request: Request, env: Env): Response | null {
    const edgeToken = request.headers.get(EdgeHeader.EDGE_TOKEN);

    // 1. [Priority 1: Private BYOC / Operator VIP Pass]
    if (env.XPHI_EDGE_TOKEN) {
        if (edgeToken === env.XPHI_EDGE_TOKEN) return null;
        if (!env.XPHI_X402_RECEIVER) {
            return new Response(JSON.stringify({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32001, message: `Unauthorized: Invalid or missing ${EdgeHeader.EDGE_TOKEN}` }
            }), {
                status: 401,
                headers: {
                    "Content-Type": "application/json",
                    [EdgeHeader.STATE]: "error"
                }
            });
        }
    }

    // 2. [Priority 2: Public x402 Monetized Gate]
    if (env.XPHI_X402_RECEIVER) {
        const fuelReceipt = request.headers.get(EdgeHeader.FUEL_RECEIPT);
        if (!fuelReceipt) {
            return new Response(JSON.stringify({
                jsonrpc: "2.0",
                id: null,
                error: { code: 402, message: `Payment Required: Attach ${EdgeHeader.FUEL_RECEIPT} (x402)` }
            }), {
                status: 402,
                headers: {
                    "Content-Type": "application/json",
                    [EdgeHeader.STATE]: "error",
                    "X-Fiber-Payment-Required": "x402-v1",
                    "X-Fiber-Payment-Address": env.XPHI_X402_RECEIVER
                }
            });
        }
    }

    // 3. [Priority 3: Open / Local Dev Default]
    return null;
}

function readCString(memory: WebAssembly.Memory, ptr: number): string {
    const memView = new Uint8Array(memory.buffer);
    let endPtr = ptr;
    while (endPtr < memView.length && memView[endPtr] !== 0) {
        endPtr++;
    }
    return new TextDecoder().decode(memView.subarray(ptr, endPtr));
}

async function executePureWasm(vmTarget: string, payload: any): Promise<string> {
    console.log(`[WASM] Initializing Wasm module for target: ${vmTarget}`);
    let targetModule = fsmModule;
    if (vmTarget === "PHASE") targetModule = phaseModule;

    const instance = await WebAssembly.instantiate(targetModule, { env: { invoke_native_vm: () => 0 } });
    const exports: any = instance.exports;
    const memory = exports.memory as WebAssembly.Memory;

    const payloadStr = JSON.stringify(payload || {});
    const payloadBytes = new TextEncoder().encode(vmTarget === "PHASE" ? payloadStr : payloadStr + "\0");
    console.log(`[WASM] Payload size: ${payloadBytes.length} bytes`);

    const codePtr = exports.alloc(payloadBytes.length);
    console.log(`[WASM] Allocated memory ptr: ${codePtr}`);
    new Uint8Array(memory.buffer).set(payloadBytes, codePtr);

    try {
        if (vmTarget === "PHASE") {
            if (typeof exports.invoke_wasm !== "function") {
                throw new Error("WASM Trap: phase.wasm does not export 'invoke_wasm'");
            }

            console.log(`[WASM] Calling invoke_wasm(ptr: ${codePtr}, len: ${payloadBytes.length})`);
            const t0 = Date.now();

            const resPacked = BigInt(exports.invoke_wasm(codePtr, payloadBytes.length));
            console.log(`[WASM] invoke_wasm returned in ${Date.now() - t0}ms | Packed res: ${resPacked}`);

            const resPtr = Number(resPacked >> 32n);
            const resLen = Number(resPacked & 0xFFFFFFFFn);
            console.log(`[WASM] Decoded resPtr: ${resPtr}, resLen: ${resLen}`);

            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from PHASE");

            const memView = new Uint8Array(memory.buffer);
            const resultStr = new TextDecoder().decode(memView.subarray(resPtr, resPtr + resLen));

            if (exports.dealloc) {
                console.log(`[WASM] Deallocating output memory (ptr: ${resPtr}, len: ${resLen})`);
                exports.dealloc(resPtr, resLen);
            }
            return resultStr;

        } else {
            if (typeof exports.execute_router !== "function") {
                throw new Error(`WASM Trap: ${vmTarget} does not export 'execute_router'`);
            }

            console.log(`[WASM] Calling execute_router(ptr: ${codePtr})`);
            const t0 = Date.now();
            const resPtr = exports.execute_router(codePtr);
            console.log(`[WASM] execute_router returned in ${Date.now() - t0}ms | resPtr: ${resPtr}`);

            if (resPtr === 0) throw new Error("WASM Execution Trap: Returned Null Pointer from DVM");

            const resultStr = readCString(memory, resPtr);
            return resultStr;
        }
    } finally {
        if (exports.dealloc) {
            console.log(`[WASM] Deallocating input memory (ptr: ${codePtr}, len: ${payloadBytes.length})`);
            exports.dealloc(codePtr, payloadBytes.length);
        }
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const fetchStart = Date.now();
        if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

        const authRejection = verifyEdgeIngress(request, env);
        if (authRejection) return authRejection;

        try {
            const input: any = await request.json();
            const reqId = input.id || "unknown";
            const vmTarget = (input.params?.vm_target || "PYTHON").toUpperCase();

            console.log(`[Router] [RECV] Request ID: ${reqId} | Target: ${vmTarget}`);

            // Handle WASM natively
            if (vmTarget === "DVM" || vmTarget === "PHASE") {
                let resultStr = "";
                try {
                    resultStr = await executePureWasm(vmTarget, input.params?.payload);
                } catch (err: any) {
                    console.error(`[Router] WASM Trap/Panic caught for ID: ${reqId}`, err.message);
                    return new Response(JSON.stringify({
                        jsonrpc: "2.0",
                        error: { message: `WASM Trap/Panic: ${err.message}` },
                        id: input.id
                    }), { headers: { "Content-Type": "application/json" } });
                }

                let isSuccess = false;
                let errorMessage = "WASM Logic Execution Failed";
                let wasmParsed = null;

                if (!resultStr || resultStr.trim() === "") {
                    isSuccess = false;
                    errorMessage = "WASM Execution blocked: Unregistered API or Silent Trap";
                    console.log(`[Router] Empty WASM result for ID: ${reqId}`);
                } else {
                    try {
                        wasmParsed = JSON.parse(resultStr);
                        if (wasmParsed.success === false || wasmParsed.error !== undefined) {
                            isSuccess = false;
                            const rawErr = wasmParsed.error;
                            if (typeof rawErr === "object" && rawErr?.message) {
                                errorMessage = rawErr.message;
                            } else {
                                errorMessage = rawErr || wasmParsed.revert_reason || "WASM Logic Execution Failed";
                            }
                            console.log(`[Router] [INTERCEPT] Wasm boundary rejected payload for ID: ${reqId}`);
                        } else {
                            isSuccess = true;
                        }
                    } catch (e) {
                        const lowerRes = resultStr.toLowerCase();
                        if (
                            lowerRes.includes("error") ||
                            lowerRes.includes("failed") ||
                            lowerRes.includes("denied") ||
                            lowerRes.includes("trap") ||
                            lowerRes.includes("panic")
                        ) {
                            isSuccess = false;
                            errorMessage = `WASM Execution Error: ${resultStr}`;
                            console.log(`[Router] [INTERCEPT] Wasm plain-text rejection matched for ID: ${reqId}`);
                        } else {
                            isSuccess = true;
                        }
                    }
                }

                const headers = { "Content-Type": "application/json" };
                console.log(`[Router] [DONE] Sending Wasm response for ID: ${reqId} | Success: ${isSuccess} | Elapsed: ${Date.now() - fetchStart}ms`);
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

            // Proxy to Python Engine + WASM Hashing Coordinator
            if (input.method === "execute" || vmTarget === "PYTHON" || input.params?.method_func === "execute_code") {
                console.log(`[Router] [PROXY] Forwarding Request ID: ${reqId} to PYTHON_ENGINE...`);
                const tProxyStart = Date.now();

                const proxyRequest = new Request("http://internal-python/execute", {
                    method: "POST",
                    body: JSON.stringify(input),
                    headers: { "Content-Type": "application/json" }
                });

                const pythonResponse = await env.PYTHON_ENGINE.fetch(proxyRequest);
                console.log(`[Router] [PROXY] PYTHON_ENGINE responded for ID: ${reqId} in ${Date.now() - tProxyStart}ms | HTTP ${pythonResponse.status}`);
                const responseText = await pythonResponse.text();
                let pyParsed: any = {};
                try {
                    pyParsed = JSON.parse(responseText);
                } catch (e) {
                    console.error(`[Router] Failed to parse Python response for ID: ${reqId}`);
                }

                // Extract execution metrics from result or error payload
                const action = input.params?.method_func || input.method || "execute_code";
                const metrics = pyParsed.result?.metrics || pyParsed.error?.data?.metrics || {
                    fuel_consumed: 1,
                    mem_usage_bytes: 1024,
                    tier: "EDGE_UNKNOWN"
                };

                // Format canonical record for phase.wasm compute_fingerprint
                const canonicalRecord = {
                    action: action,
                    tier: metrics.tier,
                    fuel: metrics.fuel_consumed,
                    mem_usage: metrics.mem_usage_bytes
                };

                const hashPayload = {
                    method: "compute_fingerprint",
                    context: { timestamp: Date.now() },
                    payload: [canonicalRecord]
                };

                // Seal execution fingerprint via phase.wasm
                let executionHash = "HASH_GENERATION_FAILED";
                try {
                    console.log(`[Router] [SEAL] Computing execution fingerprint via phase.wasm for ID: ${reqId}...`);
                    const wasmResStr = await executePureWasm("PHASE", hashPayload);
                    const wasmRes = JSON.parse(wasmResStr);

                    if (wasmRes.success && wasmRes.data) {
                        executionHash = typeof wasmRes.data === "string"
                            ? JSON.parse(wasmRes.data).fingerprint
                            : wasmRes.data.fingerprint;
                        console.log(`[Router] [SEAL] Canonical Hash Sealed: ${executionHash}`);
                    } else {
                        console.error(`[Router] WASM Hash Generation Rejected. Reason: ${wasmRes.error}`);
                    }
                } catch (e) {
                    console.error(`[Router] Hash computation failed for ID: ${reqId}`, e);
                }

                const finalHeaders = new Headers(pythonResponse.headers);
                finalHeaders.set(EdgeHeader.CANONICAL_HASH, executionHash);
                return new Response(responseText, {
                    status: pythonResponse.status,
                    headers: finalHeaders
                });
            }

            console.warn(`[Router] Unsupported method/target for ID: ${reqId} | Target: ${vmTarget}`);
            return new Response(JSON.stringify({
                jsonrpc: "2.0",
                error: { message: `Method or Target not supported: ${vmTarget}` },
                id: input.id
            }), { status: 400, headers: { "Content-Type": "application/json" } });
        } catch (e: any) {
            console.error(`[Router] Internal Router Fault:`, e);
            return new Response(JSON.stringify({
                jsonrpc: "2.0",
                error: { message: `Router Internal Error: ${e.message}` }
            }), { status: 500, headers: { "Content-Type": "application/json" } });
        }
    }
};