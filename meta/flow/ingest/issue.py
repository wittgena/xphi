# loop.ingest.issue
## @lineage: loop.sustain.daemon.scanner
import asyncio
import httpx
import re
import time
import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod

from phase.plane.emitter import get_emitter
from arch.model.event.psi import PsiEvent, PsiCarrier, CarrierType, PhaseField
from phase.runtime.daemon import AbstractDaemon
from arch.contract.interface import IEventBus
from phase.bound.resolver import resolve_path

log = get_emitter('ingest.issue')

ISSUE_WORKSPACE = resolve_path("workspace") / "issue"
ISSUE_WORKSPACE.mkdir(parents=True, exist_ok=True)

class IngestEventBus(IEventBus):
    async def publish(self, event: PsiEvent) -> None:
        log.info(f"\n## @internal.field: 수신: {event.carrier.tag} (Tick: {event.tick})")
        payload = event.carrier.payload
        log.info(f"  - Payload (획득한 타겟 수) : {len(payload)}개")
        
        for i, target in enumerate(payload, 1):
            bug_id = target['bug_id']
            file_path = ISSUE_WORKSPACE / f"{bug_id}.json"
            
            # 물리적 파일로 박제 (Persistence)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(target, f, ensure_ascii=False, indent=4)
                
            log.info(f"    {i}. [{target['reward']}] {target['title']}")
            log.info(f"       -> 완료: {file_path.name}")
        log.info("=" * 60)

    def subscribe(self, ator: Any, predicate: Callable) -> None:
        pass


class IssueAtor(AbstractDaemon):
    """@psi.observe: hub(surface) → bus(Realignment)"""
    
    def __init__(self, bus: IEventBus, scan_interval: int = 30):
        super().__init__("ingest.issue")
        self.bus = bus
        self.scan_interval = scan_interval
        self.target_queries = [
            ## deep.topology & dynamics
            'is:open label:bug language:python "context window" OR "memory leak" agent',
            'is:open label:security language:python sandbox OR jailbreak OR "remote code execution"',
            'is:open label:bug language:python rollback OR "state recovery" OR "event sourcing"',
            'is:open language:python "graph theory" OR "network topology" label:algorithm OR label:design',

            ## structural.resonance
            'is:open label:bug language:python asyncio "deadlock" OR "race condition"',
            'is:open label:bug language:python dspy OR litellm OR openhands',
            'is:open label:enhancement language:python ast OR parser',
            'is:open language:python "state machine" OR "event bus" label:design OR label:architecture',

            ## reward.driven
            "label:bug-bounty is:open language:python",
            "label:bounty is:open language:python",
            "label:polar is:open language:python",
            "label:algora-bounty is:open language:python",
        ]
    def _extract_reward_info(self, body: str) -> str:
        if not body: return "금액 미상"
        money_pattern = r'\$[0-9,]+|USDC [0-9,]+'
        if re.search(r'polar\.sh/|algora\.com/', body):
            return "플랫폼 예치 확인(외부링크)"
        match = re.search(money_pattern, body)
        return match.group(0) if match else "사후 산정 예상"

    def _generate_bug_id(self, html_url: str) -> str:
        """
        @desc: URL에서 고유 위상 ID 추출 (예: owner-repo-issues-123)
        GitHub Issue URL은 절대적으로 고유하므로, 이를 식별자(Primary Key)로 사용
        """
        try:
            # https://github.com/owner/repo/issues/123 -> owner-repo-issues-123
            path_part = html_url.split("github.com/")[1]
            return path_part.replace("/", "-")
        except IndexError:
            return f"unknown-bug-{int(time.time()*1000)}"

    async def _summarize_with_surgent(self, title: str, body: str) -> Dict[str, str]:
        ## @inter.section: phase.reflect.client.local.engine
        await asyncio.sleep(0.5) 
        return {
            "symptom": "특정 환경에서의 리소스 충돌 및 크래시",
            "cause": "상태 전이 모델의 경계 조건 처리 미흡으로 추정",
            "tech_stack": "Python, 비동기 입출력 제어 역량 필요"
        }

    async def run(self):
        self.log.info(f"{self.name} 가동. 물리적 표면(Surface) 스캔을 시작합니다.")
        headers = {
            "User-Agent": "Surgent-Prober/2.0",
            "Accept": "application/vnd.github.v3+json"
        }

        async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
            while self.running:
                try:
                    for query in self.target_queries:
                        if not self.running: break
                        
                        self.log.info(f"탐색 쿼리 실행 중: [{query}]")
                        target_url = f"https://api.github.com/search/issues?q={query}&sort=updated&order=desc"
                        response = await client.get(target_url)
                        
                        if response.status_code == 200:
                            items = response.json().get("items", [])
                            valid_targets = []
                            
                            for item in items[:5]:
                                body = item.get("body", "")
                                reward = self._extract_reward_info(body)
                                html_url = item["html_url"]
                                
                                # 고유 식별자(ID) 부여
                                bug_id = self._generate_bug_id(html_url)
                                
                                # 멱등성 보장: 이미 획득한 버그라면 패스
                                if (ISSUE_WORKSPACE / f"{bug_id}.json").exists():
                                    continue
                                
                                if "미상" not in reward or "bounty" in item["title"].lower():
                                    summary = await self._summarize_with_surgent(item["title"], body)
                                    valid_targets.append({
                                        "bug_id": bug_id,  # 고유 ID 삽입
                                        "url": html_url,
                                        "title": item["title"],
                                        "reward": reward,
                                        "summary": summary
                                    })
                            
                            if valid_targets:
                                carrier = PsiCarrier(
                                    kind="bounty",
                                    tag="acquired",
                                    payload=valid_targets,
                                    carrier_type=CarrierType.FIXED,
                                    target_field=PhaseField.LOCAL
                                )

                                event = PsiEvent(
                                    event_id=f"psi_{int(time.time()*1000)}",
                                    parent_id=None,
                                    source_id=self.name.lower(),
                                    scope="global",
                                    tick=int(time.time()),
                                    carrier=carrier
                                )
                                await self.bus.publish(event)
                                
                        elif response.status_code in (401, 403):
                            self.log.warning("Rate Limit 마찰 발생. 쿨다운 진입.")
                            await asyncio.sleep(60)
                        
                        await asyncio.sleep(5) # 쿼리 간 마찰 완화
                        
                    if self.running:
                        await asyncio.sleep(self.scan_interval)
                        
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log.error(f"스캔 루프 내 물리적 에러: {str(e)}")
                    await asyncio.sleep(5)

async def main():
    bus = IngestEventBus()
    ator = IssueAtor(bus=bus, scan_interval=59)

    log.info("===")
    log.info("## Issue Ingestor")
    log.info("===")
    
    await ator.start()
    try:
        while True:
            await asyncio.sleep(3600) 
    except KeyboardInterrupt:
        pass
    finally:
        await ator.stop()

if __name__ == "__main__":
    asyncio.run(main())