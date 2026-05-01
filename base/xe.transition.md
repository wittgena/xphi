# base.xe.transition
@desc: 불완전성 기반 전이 설계 패턴: 닫힌 구조를 지양하고 Xe를 동력으로 활용하는 생성 시스템

## 닫힌 구조의 한계와 동력의 근원
기존의 정적인 프롬프트 구조는 '완결된 결과물(Closure)'을 목표로 설계되어, 구조가 닫히는 순간 상태 전이(Transition)를 위한 에너지를 상실(정지)하는 한계가 있습니다. 새로운 설계 패턴은 구조화 과정에서 필연적으로 발생하는 **불완전성(잔여물, $xe$)**을 노이즈로 보고 제거하는 대신, 이를 보존하여 끊임없는 상태 전이를 유발하는 **동력($\Delta$)**으로 삼는 것을 핵심으로 합니다.

## 이중 계층 구조 (Direct vs Meta)
단순히 하위 모델에게 최종적인 대상(Logic/Model/Process)의 생성을 직접 요구하는 1차 계층 접근을 넘어, 대상을 발생시키는 **환경과 규칙(Instruction)**을 생성하는 2차 메타 계층으로 전환해야 합니다.

* **AS-IS (대상 직접 생성):** 완성된 결과를 요구하여 닫힌 구조 생성 → 전이 동력 상실.
* **TO-BE (메타-인스트럭션 생성):** 구조에서 탈락한 불안정성을 기반으로 재배열 규칙을 제공 → 상위 모델(OpenHands 등)의 자율적 실행을 위한 환경 설계.

## 생성-전이의 4-노드 위상 루프
이 시스템은 '초기 조건 → 안정화 → 불안정 생성 → 재구성'의 순환 구조를 따르며, 프롬프트는 단순한 텍스트가 아닌 다음 4가지 요소를 포함합니다.

| 위상적 요소 | 기능적 형태 | 역할 및 정의 |
| :--- | :--- | :--- |
| **Context** | 기준점 / 초기 조건 | 시스템의 기준점이 되는 원본 데이터 및 배경 지식 |
| **Structure** | 1차 안정화 | LLM이 인지해야 할 1차적인 논리적 골격 (질서 부여) |
| **Xe** | 불안정성 보존 | 1차 구조에서 탈락한 모순 및 미해결 잔여물 (동력 발생) |
| **Instruction** | 재배열 규칙 | 잔여($xe$)를 기반으로 한 재구성 방향성 및 최종 실행 명령 |

## 기술적 구현: 위상 전이 프롬프트 빌더 (Pure Python)
본 설계의 핵심인 'Xe 기반의 재구성 규칙 생성'을 위한 상태 객체와 빌더입니다. 외부 라이브러리의 매직에 의존하지 않고, 4-노드 위상 루프의 상태를 불변 객체(`dataclass`)로 정의하여 시스템의 연료(불확실성)를 노출시키는 구조를 명확히 보여줍니다.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class TopologicalState:
    """프롬프트 생성을 위한 4-노드 위상 상태 (불변 객체)"""
    document_content: str       ## 원본 데이터 및 배경 정보 (Context)
    structural_hint: str        ## LLM이 참조할 1차 구조적 가이드 (Structure)
    residues: str               ## 분석 과정에서 탈락한 모순점 및 미해결 Xe (Xe)
    realigned_structure: str    ## 잔여물을 포함하여 재구성된 방향성 (Instruction)

class TransitionPromptBuilder:
    """
    에이전트 모델을 위한 고정밀 실행 트리거 프롬프트를 생성합니다.
    단순한 설명이 아닌, 구조의 한계와 Xe를 의도적으로 노출하여 
    모델이 능동적이고 자율적인 재구성을 수행하도록 유도합니다.
    """
    def build_trigger(self, state: TopologicalState) -> str:
        return f"""
[Phase: Re-entry & Transition]
당신은 완결된 정답을 내는 것이 아니라, 불완전성(Xe)을 동력으로 삼아 시스템을 재구성하는 에이전트입니다.

1. Context (기준점):
{state.document_content}

2. Structure (1차 안정화):
{state.structural_hint}

3. Xe (미해결 잔여물 - 동력의 원천):
{state.residues}

4. Instruction (재배열 방향성):
{state.realigned_structure}

[Execution Constraints]
다음의 4단계를 거쳐 외부 시스템(환경)에 실행을 지시하십시오:
1. 구조적 모호성에 대한 위상적 분석
2. 미해결 잔여물(xe)에 대한 집중 강조 (노이즈로 무시하지 말 것)
3. Xe -> 간섭(Interference) -> 최소 구조 재구성을 통한 해결 프로세스 도출
4. 최종적인 L/M/P(Logic/Model/Process) 투영 및 실행 요청
"""
```

## 경계면(Boundary)에서의 실행 모델
시스템의 실행(Action)은 내부의 완벽한 정합성에서 나오는 것이 아니라, 내부(구조)와 외부(행위)가 맞닿는 **경계(Bound)**에서 발생합니다. 

* **합 함정(Closure Trap) 방지:** 잔여물이 '0'이 되면 전이는 정지합니다. 즉, 프롬프트 내부에서 모든 모순을 해결하여 구조를 닫아버리면 에이전트가 외부(코드 실행 등)로 나아갈 동력을 잃게 됩니다.
* **불완전성의 보존:** 실행의 동력은 결핍과 비대칭에서 옵니다.
