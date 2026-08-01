# watcher.xelog.depend
from fastapi import Request

from arch.topos.tunnel.subs import DistributedPubSub
from arch.kernel.gov.ingress.policy import (
    IngressPolicyEngine, 
    ToposSequencer, 
    FuelAllocator, 
    HealthMonitor
)
from watcher.kernel.audit.ledger import AuditLedger
from watcher.kernel.log.store import LogStreamStore
from watcher.xelog.profile import BenchProfile
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.anchor import NexusAnchor
from watcher.dphi.adapter.exchange import ExchangeAdapter
from watcher.dphi.adapter.sign import NodeSigner


## WASM & Core Compute
async def get_wasm_broker(request: Request) -> WasmBroker:
    return request.app.state.broker

## Ledger & State Persistence
async def get_logstream_store(request: Request) -> LogStreamStore:
    """앱 구동 시 초기화된 Immutable Ledger Store 싱글톤 주입"""
    return request.app.state.store

## Consensus & Anchor
async def get_nexus_anchor(request: Request) -> NexusAnchor:
    """WASM 패리티 검증 및 Epoch Sealing을 전담하는 Anchor 주입"""
    broker = await get_wasm_broker(request)
    allowed_committee = getattr(request.app.state.config, "committee_pubs", [])
    return NexusAnchor(broker=broker, consensus_threshold=1, allowed_committee=allowed_committee)

## DeFi & Financial Adapters
async def get_exchange_adapter(request: Request) -> ExchangeAdapter:
    """환전소/정산 영수증 발급 어댑터 주입"""
    node_pubkey = NodeSigner.get_instance().pubkey_hex
    return ExchangeAdapter(clearing_house_pub_key=node_pubkey)

## Profiling & Billing (A2A Compute 연산 전용)
async def get_bench_profile() -> BenchProfile:
    """A2A 연산에 대한 과금 검증 및 Cgroup 티어 산출/실행기 주입"""
    return BenchProfile()

## Ingress Policy (D3Fi & Gateway)
async def get_ingress_policy(request: Request) -> IngressPolicyEngine:
    """거래 인입 시 위상(Topo), 자원(Fuel), 상태(Health)를 판단하는 단일 엔진"""
    return IngressPolicyEngine(
        sequencer=ToposSequencer(),
        allocator=FuelAllocator(),
        monitor=HealthMonitor()
    )

## Event Driven & Streaming (OTLP / Audit)
async def get_pubsub(request: Request) -> DistributedPubSub:
    """글로벌 브로드캐스트 및 이벤트 파이프라인 주입"""
    return request.app.state.pubsub

## Ecosystem & Audit
async def get_audit_ledger(request: Request) -> AuditLedger:
    """PII 마스킹 및 감사 로그 기록기 주입"""
    return request.app.state.audit_ledger