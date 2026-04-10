package subst.theoria.phi.runtime

import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import subst.bound.LogSignal
import subst.bound.SignalBound
import subst.theoria.judgment.*
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

object ContextKeys {
    const val INTERFERE_ALIGN = "interfere.align"
    const val LAST_FLOW_FROM = "lastFlow.from"
    fun pathLast() = "path.last"
}

sealed interface JudgmentResult {
    object Term : JudgmentResult
    object Collapse : JudgmentResult
    object Drift : JudgmentResult
}

class PhiRuntimeContext(
    private val parent: PhiRuntimeContext? = null,
    private val memory: MutableMap<String, Any> = ConcurrentHashMap()
) {
    operator fun get(key: String): Any? = memory[key] ?: parent?.get(key)
    operator fun set(key: String, value: Any) { memory[key] = value }

    // 블록이 끝날 때 메모리를 정리할 수 있는 자식 스코프 생성
    fun childScope() = PhiRuntimeContext(parent = this)
}



@Component
class PhiInterpreter(
    private val backgroundScope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob()),
    private val signalBound: SignalBound,
    private val redis: ReactiveRedisTemplate<String, String>? = null,
    private val webClient: WebClient? = null
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Ψ → Φ′ (sequential phase evaluation)
     * - step 단위는 순차적
     * - 내부에서 Φx는 잔류(field)로 분기됨
     */
    suspend fun run(
        judgment: PhiJudgment,
        runtimeContext: PhiRuntimeContext = PhiRuntimeContext()
    ): JudgmentResult {
        log.info("[AUG] interpreter start")
        for (step in judgment.steps) {
            val result = eval(step, runtimeContext)
            if (result != JudgmentResult.Term) {
                log.info("[UAA] terminated: {}", result)
                return result
            }
        }
        log.info("[UGA] completed")
        return JudgmentResult.Term
    }

    /** Φ′ evaluation (step scope) */
    private suspend fun eval(
        step: StepDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        log.info("[step] {}", step.name)
        for (decl in step.body) {
            when (decl) {
                is FlowDecl -> handleFlow(decl, ctx)
                is PhaseDecl -> return evalPhase(decl, ctx)
                is CollapseDecl -> { handleCollapse(decl, ctx) }
                is FlowBlock -> return evalBlock(decl, ctx)
            }
        }
        return JudgmentResult.Term
    }

    /**
     * Ψ → Φ′ + Φx
     * - ctx mutation → Φ′
     * - redis/webClient → Φx (residual field)
     * - launch 사용으로 기존 subscribe 의미 유지
     */
    private suspend fun handleFlow(
        decl: FlowDecl,
        ctx: PhiRuntimeContext
    ) = coroutineScope {
        val nodeName = decl.from.name
        val targetName = decl.to.name
        log.info("[flow] transition from {}", nodeName)

        ctx["lastFlow.from"] = nodeName
        ctx["lastFlow.to"] = decl.to.name
        backgroundScope.launch {
            try {
                val analog = LogSignal(amplitude = extractAmplitude(decl, ctx))
                val job = signalBound.bind(
                    scope = this,               // ← 현재 coroutine scope 귀속
                    source = analog,
                    targetName = targetName,
                    ctx = ctx
                )
                // lifecycle trace (optional, overwrite 허용)
                ctx["junction.$nodeName"] = job
            } catch (e: Exception) {
                log.warn("[Φ:lock] binding error: {}", e.message)
            }
        }

        val r = redis ?: return@coroutineScope
        val adjKey = "space:adj:$nodeName"
        val intensityKey = "space:adj:intensity:$nodeName"
        val ttl = Duration.ofSeconds(60)

        /** Φx-1: adjacency enrichment (residual) - 늦게 도착해도 허용 (drift) */
        backgroundScope.launch {
            try {
                val neighbors = r.opsForSet()
                    .members(adjKey)
                    .collectList()
                    .awaitSingle()

                val resolved =
                    if (neighbors.isEmpty() && webClient != null) {
                        fetchFromExternal(adjKey)
                    } else neighbors

                if (resolved.isNotEmpty()) {
                    ctx["adj.neighbors.$nodeName"] = resolved
                }
            } catch (e: Exception) {
                log.debug("[adj] enrichment failed: {}", e.message)
            }
        }

        /** Φx-2: intensity pumping (decaying field) */
        launch {
            try {
                r.opsForValue().increment(intensityKey).awaitSingle()
                r.expire(intensityKey, ttl).awaitSingle()
            } catch (e: Exception) {
                log.warn("[intensity] failed: {}", nodeName)
            }
        }
    }

    private suspend fun fetchFromExternal(key: String): List<String> {
        /** external fallback (optional field extension) */
        return try {
            webClient?.get()
                ?.uri { it.path("/query").queryParam("key", key).build() }
                ?.retrieve()
                ?.bodyToMono(Map::class.java)
                ?.awaitSingle()
                ?.let { resp ->
                    (resp["neighbors"] as? List<*>)?.map { it.toString() }
                        ?: emptyList()
                } ?: emptyList()
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun handleCollapse(
        decl: CollapseDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        log.info(
            "[collapse] {} → {}",
            decl.sources.joinToString { it.name },
            decl.bound.name
        )
        ctx["collapsed"] = true
        ctx["collapse.product"] = decl.bound.name
        return JudgmentResult.Collapse
    }

    private fun extractAmplitude(
        decl: FlowDecl,
        ctx: PhiRuntimeContext
    ): Double {
        // 최소 예시 (구조 보존 목적)
        return when {
            decl.from.name.contains("alpha") -> 0.8
            else -> 0.3
        }
    }
    private suspend fun evalPhase(
        phase: PhaseDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        log.info("[phase] {}", phase.name)
        phase.bind?.let {
            ctx["bind.nodes.${phase.name}"] = it.nodes
            ctx["bind.edges.${phase.name}"] = it.edges
        }
        return phase.flow.evalAll(ctx)
    }

    private suspend fun evalBlock(
        block: FlowBlock,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        return when (block) {
            is LoopDecl -> evalLoop(block, ctx)
            is CascadeDecl -> evalCascade(block, ctx)
            is PathDecl -> {
                ctx["path.last"] = block.sequence.map { it.name }
                JudgmentResult.Term
            }
            is InputDecl -> {
                ctx["input.psi"] = block.psi.name
                JudgmentResult.Term
            }
            is InterfereDecl -> evalInterfere(block, ctx)
            is ReflectDecl -> evalReflect(block, ctx)
            else -> JudgmentResult.Term
        }
    }

    private suspend fun evalLoop(
        loop: LoopDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        log.info("[loop] {}", loop.name)
        return loop.blocks.evalAll(ctx) // 단 한 줄로 축소!
    }

    private suspend fun evalCascade(
        cascade: CascadeDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        log.info("[cascade] {}", cascade.name)
        for (stage in cascade.stages) {
            ctx["cascade.stage"] = stage.sequence.map { it.name }
        }
        for (action in cascade.result) {
            evalAction(action, ctx)
        }
        return JudgmentResult.Term
    }

    private suspend fun evalInterfere(
        block: InterfereDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        for (op in block.operations) {
            when (op) {
                is AlignOp -> {
                    val aligned = op.left.name == op.right.name
                    ctx["interfere.align"] = aligned
                }
                is PhaseShiftOp -> { ctx["interfere.phaseShift"] = true }
            }
        }
        return JudgmentResult.Term
    }

    private fun evalReflect(
        block: ReflectDecl,
        ctx: PhiRuntimeContext
    ): JudgmentResult {
        for (cond in block.conditions) {
            var matched = false
            when (cond) {
                is IfAligned -> {
                    if (ctx["interfere.align"] == true) {
                        cond.actions.forEach { evalAction(it, ctx) }
                        matched = true
                    }
                }
                is IfEqual -> {
                    val equal = cond.left.name == cond.right.name
                    if (equal) {
                        cond.actions.forEach { evalAction(it, ctx) }
                        matched = true
                    }
                }
                is ElseBlock -> {
                    // 앞선 조건들이 하나도 맞지 않았을 때만 실행
                    cond.actions.forEach { evalAction(it, ctx) }
                    matched = true
                }
            }
            // 조건이 매칭되어 실행되었다면 나머지 조건(else 등)은 평가하지 않고 종료
            if (matched) break
        }
        return JudgmentResult.Term
    }

    private fun evalAction(action: ActionDecl, ctx: PhiRuntimeContext) {
        when (action) {
            is EmitDecl -> { ctx["emit"] = action.expr.toString() }
            is AbsorbDecl -> { ctx["absorb"] = action.target.name }
            is InjectDecl -> { ctx["inject"] = action.target }
            GainIncreaseDecl -> {
                val current = (ctx["gain"] as? Int ?: 0)
                ctx["gain"] = current + 1
            }
        }
    }

    private suspend fun Iterable<PhiDecl>.evalAll(ctx: PhiRuntimeContext): JudgmentResult {
        for (decl in this) {
            val result = when (decl) {
                is FlowBlock -> evalBlock(decl, ctx)
                is PhaseDecl -> evalPhase(decl, ctx)
                is FlowDecl -> handleFlow(decl, ctx).let { JudgmentResult.Term }
                is CollapseDecl -> handleCollapse(decl, ctx)
                else -> JudgmentResult.Term
            }
            if (result != JudgmentResult.Term) return result
        }
        return JudgmentResult.Term
    }
}