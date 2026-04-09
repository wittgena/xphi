package subst.theoria.phi.executor

import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import subst.theoria.judgment.*
import kotlin.coroutines.coroutineContext

interface PhiExecutor {
    fun execute(phi: PhiJudgment): Flow<JudgmentEvent>
}

@Component
class FlowExecutor : PhiExecutor {
    private val log = LoggerFactory.getLogger(javaClass)
    override fun execute(phi: PhiJudgment): Flow<JudgmentEvent> =
        channelFlow {
            val ctx = ExecutionContext(this)
            for (step in phi.steps) { ctx.runStep(step, ctx) }
        }
}

sealed class JudgmentEvent {
    data class FlowEvent(val psi: Psi, val phi: Phi, val boundary: Bound, val phase: String) : JudgmentEvent()
    data class CollapseEvent(val sources: List<Psi>, val surface: Phi, val phase: String) : JudgmentEvent()
}

class ExecutionContext(
    private val producer: ProducerScope<JudgmentEvent>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private var currentPhase: String = "INIT"

    suspend fun phase(name: String) {
        currentPhase = name
    }

    suspend fun runStep(step: StepDecl, ctx: ExecutionContext) {
        ctx.phase(step.name)
        for (decl in step.body) {
            when (decl) {
                is FlowDecl -> { ctx.emitFlow(psi = decl.from, phi = decl.to, bound = decl.bound) }
                is CollapseDecl -> { ctx.emitCollapse(sources = decl.sources, bound = decl.bound) }
                else -> { log.warn("[runStep] unknown decl: $decl") }
            }
        }
    }

    suspend fun emitFlow(psi: Psi, phi: Phi, bound: Bound) {
        JudgmentEvent.FlowEvent(psi = psi, phi = phi, boundary = bound, phase = currentPhase).let {
            producer.send(it)
        }
    }

    suspend fun emitCollapse(sources: List<Psi>, bound: Phi) {
        JudgmentEvent.CollapseEvent(sources = sources, surface = bound, phase = currentPhase).let {
            producer.send(it)
        }
    }

    suspend fun rhythm(
        interval: Long = 1000,
        condition: () -> Boolean = { true },
        block: suspend ExecutionContext.() -> Unit
    ) {
        while (coroutineContext.isActive && condition()) {
            block()
            delay(interval)
        }
    }
}