package subst.theoria.flow

import kotlinx.coroutines.flow.Flow
import subst.theoria.judgment.*
import subst.theoria.phi.executor.FlowExecutor
import subst.theoria.phi.executor.JudgmentEvent
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

// Host: restriction-modification system을 가진 숙주.
data class Host(
    val recognitionStrength: Double,
    val methylationLevel: Double,
    var damage: Double = 0.0
)

data class Phage(
    var resistance: Double,
    var replicationRate: Double
)

data class InteractionState(
    var phageLoad: Double = 1.0,
    var time: Int = 0
)

// Restriction-Modification system과 phage 간의 확률적 동역학 경쟁을 PhiJudgment으로 변환
class RMFlow(
    private val host: Host,
    private val phage: Phage,
    private val state: InteractionState,
    private val maxSteps: Int = 50
) {

    /**
     * - 결과를 PhiJudgment으로 구성한다.
     * - 실제 시간 반복은 while 루프로 계산되지만,
     * - 엔진에는 오직 위상 이벤트만 전달된다.
     */
    private fun buildPhiJudgment(): PhiJudgment {
        val decls = mutableListOf<PhiDecl>()

        /**
         * 반복 조건:
         * - 시간 제한
         * - phage 완전 제거 전
         * - host 완전 붕괴 전
         */
        while (
            state.time < maxSteps &&
            state.phageLoad > 0.01 &&
            host.damage < 1.0
        ) {
            state.time++

            /**
             * Recognition pressure:
             * - 숙주의 인식 능력 × phage 회피 실패 확률
             * > phage resistance가 높을수록 recognitionPressure는 감소.
             */
            val recognitionPressure = host.recognitionStrength * (1.0 - phage.resistance)

            /**
             * Effective cleavage:
             * - recognition에서 methylation 보호 효과를 뺀 값.
             * - 실제 절단 가능성의 근사값.
             */
            val effectiveCleavage = recognitionPressure - host.methylationLevel
            val cleavageProbability = max(0.0, min(1.0, effectiveCleavage))
            val cleavage = Random.nextDouble() < cleavageProbability
            if (cleavage) {
                /**
                 * Restriction event:
                 * - phage genome 절단 → 개체수 감소
                 * - 이는 위상적으로 "host-defense 우세" 상태.
                 */
                state.phageLoad *= 0.5

                decls += FlowDecl(
                    from = Psi("cleavage"),
                    to = Phi("phage-reduced"),
                    bound = Bound("host-defense")
                )

            } else {

                /**
                 * Replication event:
                 * - 절단 실패 → phage 증식
                 * - 이는 위상적으로 "viral-dynamics 우세" 상태.
                 */
                state.phageLoad *= phage.replicationRate

                decls += FlowDecl(
                    from = Psi("replication"),
                    to = Phi("phage-expanded"),
                    bound = Bound("viral-dynamics")
                )
            }

            /**
             * 숙주 손상 누적:
             * - phageLoad가 클수록 세포 손상 증가.
             * - 이는 장기적으로 lytic collapse를 유도.
             */
            host.damage += state.phageLoad * 0.01
        }

        /**
         * 시스템의 종결 상태 결정:
         * - Restriction victory
         * - Lytic collapse
         * - Stable coexistence
         */
        val outcome =
            when {
                state.phageLoad < 0.01 -> "restriction-victory"
                host.damage > 1.0 -> "lytic-collapse"
                else -> "stable-coexistence"
            }

        // CollapseDecl: 연속적 경쟁이 하나의 거시적 결과 상태로 수렴하는 지점.
        decls += CollapseDecl(
            sources = listOf(Psi("rm-dynamics")),
            bound = Phi(outcome)
        )

        return PhiJudgment(
            steps = listOf(
                StepDecl(
                    name = "rm.interaction",
                    body = decls
                )
            )
        )
    }

    // 외부에서는 단순히 Flow<PhaseEvent>로 소비 가능.
    fun execute(): Flow<JudgmentEvent> {
        val judgment = buildPhiJudgment()
        return FlowExecutor().execute(judgment)
    }
}