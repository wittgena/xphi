package subst.theoria.judgment.junction

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import subst.theoria.phi.runtime.PhiRuntimeContext

private val log = LoggerFactory.getLogger("subst.theoria.phi.junction.Flow")

/**
 * [ FlowJunction ]
 * 입자 방출 및 유체 역학 메타포를 위한 이산 위상(Discrete Phase) 제어 확장.
 * * LockJunction이 아날로그 파동의 지속적인 동기화를 담당한다면,
 * 본 컴포넌트는 닫힌 계(System)를 흐르는 이벤트(입자)의 생명주기, 누출, 간섭, 그리고 붕괴를 제어/추적합니다.
 */

/**
 * 1. 점화 (Ignite)
 * 잠재적 에너지(Cold Flow)에 불을 붙여 반응을 시작시키고, 컨텍스트에 생명주기를 기록합니다.
 */
fun <T> Flow<T>.ignite(
    scope: CoroutineScope,
    ctx: PhiRuntimeContext,
    phaseName: String
): Job = scope.launch {
    log.debug("[Φ:ignite] Phase '{}' ignited", phaseName)
    ctx["flow.$phaseName.status"] = "IGNITED"
    try {
        collect()
    } finally {
        log.debug("[Φ:extinguish] Phase '{}' extinguished", phaseName)
        ctx["flow.$phaseName.status"] = "EXTINGUISHED"
    }
}

/**
 * 2. 전조 누출 (Spill)
 * 본 흐름이 쏟아지기 전, 경계면 밖으로 선행 입자(Precursor)를 한 방울 흘려보냅니다.
 * 다음 위상이 본 흐름을 대비할 수 있도록 트리거 역할을 합니다.
 */
fun <T> Flow<T>.spill(
    precursor: T,
    ctx: PhiRuntimeContext,
    phaseName: String
): Flow<T> = flow {
    log.trace("[Φ:spill] Precursor particle leaked from '{}'", phaseName)
    ctx["flow.$phaseName.spilled"] = true
    emit(precursor)
    emitAll(this@spill)
}

/**
 * 3. 굴절 및 간섭 (Refract) - *New*
 * 런타임 컨텍스트의 밀도나 상태에 따라 입자의 흐름을 굴절시키거나 차단(필터링)하는 경계면입니다.
 */
fun <T> Flow<T>.refract(
    ctx: PhiRuntimeContext,
    condition: suspend (T) -> Boolean
): Flow<T> = this.filter { particle ->
    val allowed = condition(particle)
    if (!allowed) {
        // 간섭으로 인해 소멸된 입자 추적
        val dropped = ctx["refract.dropped"] as? Int ?: 0
        ctx["refract.dropped"] = dropped + 1
    }
    allowed
}

/**
 * 4. 위상 흡수 (Absorb Into)
 * 스트림이 최종 목적지(Sink)에 도달하여 위상 공간으로 완전히 흡수(소진)되는 과정입니다.
 */
fun <T> Flow<T>.absorbInto(
    scope: CoroutineScope,
    ctx: PhiRuntimeContext,
    targetName: String
): Job = scope.launch {
    log.debug("[Φ:absorb] Flow is being absorbed into '{}'", targetName)
    collect {
        val currentCount = ctx["absorb.$targetName.count"] as? Int ?: 0
        ctx["absorb.$targetName.count"] = currentCount + 1
    }
    ctx["absorb.$targetName.status"] = "SATURATED" // 포화 상태 기록
}

/**
 * 5. 위상 붕괴 포착 (Catch Collapse)
 * 단순한 예외 처리가 아닙니다. 파이프라인이 압력을 견디지 못하고 물리적으로 붕괴하는 현상을
 * 포착하여, 시스템 전체 상태를 '붕괴(Collapsed)'로 전이시킵니다.
 */
fun <T> Flow<T>.catchCollapse(
    ctx: PhiRuntimeContext,
    phaseName: String,
    fallback: suspend (Throwable) -> Unit
): Flow<T> = this.catch { e ->
    log.error("[Φ:collapse] Topological collapse detected at '{}': {}", phaseName, e.message)
    ctx["collapsed"] = true
    ctx["collapse.origin"] = phaseName
    ctx["collapse.reason"] = e.message ?: "Unknown topological anomaly"

    fallback(e)
}

/**
 * 6. 해체/전개 (Unfold)
 * 스레드나 작업을 폭력적으로 강제 종료(Cancel)하는 것이 아니라,
 * 엮여있던 위상을 안전하게 풀어서(Unfold) 자연계로 돌려보냅니다.
 */
fun Job.unfold(ctx: PhiRuntimeContext? = null, phaseName: String = "unknown") {
    log.debug("[Φ:unfold] Unfolding phase '{}'", phaseName)
    ctx?.set("flow.$phaseName.status", "UNFOLDED")
    this.cancel(CancellationException("Phase unfolded organically"))
}