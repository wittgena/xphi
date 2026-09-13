# XPHI

@desc: Core Infrastructure Kernel

XPHI is a deterministic core kernel and distributed state management engine. While Fiber handles external LLM integrations and gateway proxies, XPHI ensures infrastructure stability through hardware-level WASM isolation, FSM-based state control, and comprehensive observability.

## Directory Structure

XPHI is organized into five core namespaces for functional isolation:

* `arch/` (Contracts & Models): Asynchronous event mesh for distributed nodes, smart contract registries, and shared data models
* `kernel/` (Core Execution): Resource control (cgroups), native WASM sandboxing, and daemon/task lifecycle management
* `state/` (Determinism & Runtime): Strict Finite State Machines (FSM) to prevent race conditions, consensus ledgers, and multi-runtime control
* `watcher/` (Control Plane): Secure MCP servers, distributed tracing, chaos engineering (fault injection), and observability metrics

## Key Features

* Sandboxing: Ephemeral runtime environments designed to prevent memory leaks and unauthorized access
* FSM Engine: Manages all agent intents and transactions through predictable, sequential state machines
* Built-in Resilience: Continuous resilience validation through automated fault injection testing