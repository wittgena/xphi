package subst.theoria.judgment.junction

import kotlinx.coroutines.*
import org.springframework.stereotype.Component
import kotlin.math.abs

/**
 * PhaseLockJunction
 *
 * 위상적 결합 경계 모델 (bphi.lock)
 * - 입력 위상 흐름과 출력 흐름을 맞물려 지속 동기화
 * - 위상 오차 보정, 과부하 감지, 붕괴 방지 포함
 */
@Component
class LockJunction(
    private val tolerance: Double = 0.05,     // 허용 가능한 위상 오차
    private val overloadLimit: Double = 1.5   // 허용 가능한 최대 부하
) {
    fun bind(
        scope: CoroutineScope,
        input: LockSignal,
        output: LockSignal,
        onDrift: suspend (Double) -> Unit = {},
        onCollapse: suspend () -> Unit = {},
        transfer: suspend (LockSignal) -> LockSignal
    ): Job = scope.launch {

        var locked = alignPhase(input, output)
        if (!locked) return@launch

        while (isActive && locked) {

            val drift = calculateDrift(input, output)

            if (drift > tolerance) {
                onDrift(drift)
                correctPhase(input, output)
            }

            if (input.load > overloadLimit) {
                onCollapse()
                locked = false
                break
            }

            val transferred = transfer(input)
            output.update(transferred)

            delay(250)
        }
    }

    private fun alignPhase(inSig: LockSignal, outSig: LockSignal): Boolean {
        val diff = abs(inSig.phase - outSig.phase)
        return diff <= tolerance
    }

    private fun calculateDrift(inSig: LockSignal, outSig: LockSignal): Double {
        return abs(inSig.phase - outSig.phase)
    }

    private fun correctPhase(inSig: LockSignal, outSig: LockSignal) {
        val avg = (inSig.phase + outSig.phase) / 2
        inSig.phase = avg
        outSig.phase = avg
    }
}

/**
 * 위상 흐름 신호 (Psi 흐름 단위)
 */
data class LockSignal(
    val name: String,
    var phase: Double,     // 0.0 ~ 1.0 범위
    var amplitude: Double,
    var load: Double
) {
    fun update(from: LockSignal) {
        this.phase = from.phase
        this.amplitude = from.amplitude
        this.load = from.load
    }
}
