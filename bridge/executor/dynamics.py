# bridge.executor.dynamics
from __future__ import annotations
import asyncio
import math
import random
from typing import List, Dict, Optional, Any, Type, Callable
from interface.pir import PsiCarrier, PsiEvent
from bridge.executor.base import BaseExecutor

class DynamicsExecutor(BaseExecutor):
    """@role: WatcherSystem을 RuntimeNode의 Executor 인터페이스에 맞추는 어댑터"""
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__()
        self.config = config_dict
        self.system = None ## 지연 초기화 대상

    async def execute(self, psi: PsiEvent) -> List[PsiEvent]:
        if self.system is None:
            self.system = SystemBuilder.build(self.config)
            
        return await self.system.process_step(psi)