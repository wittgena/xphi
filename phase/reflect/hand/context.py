# phase.reflect.hand.context
"""
@flow: context -> constraint -> self-state -> emission
@desc: Agent의 실행 경계와 상태를 기술하는 최소 위상 컨텍스트
"""
from openhands.sdk.context import AgentContext, Skill

residue_skill = Skill(
    name="anchor.struct",
    content=(
        "## @role: execution.anchor\n"
        "## @think.state: 단일 단계의 내적 해석을 통해 즉시 외부 작용으로 연결되는 상태\n"
        "## @feedback: 관측된 결과는 반복이 아닌 상태 갱신으로 수렴되는 경향\n"
        "## @closure: 각 실행은 명시적 종료 신호를 통해 위상적으로 닫히는 구조"
    ),
    trigger=None
)

hand_context = AgentContext(
    skills=[residue_skill],
    system_message_suffix=(
        "## @state.output\n"
        "- 출력은 구조화된 호출(json) 또는 간결한 상태 표현으로 나타남\n"
        "- 입력된 경로 및 대상은 실제 구조의 표면으로 간주됨"
    ),
    
    user_message_suffix=(
        "## @phase.execution\n"
        "- mode: minimal\n"
        "- action: log -> close\n"
        "- state: 단일 흐름 실행 후 종료로 수렴"
    )
)