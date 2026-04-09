package subst.theoria.judgment.junction

import arrow.core.Either
import arrow.core.raise.Raise
import arrow.core.raise.either
import subst.theoria.judgment.junction.LoopJunction.propagate
import subst.theoria.judgment.junction.PhaseCollapse.*
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import subst.bound.psi.PsiEvent

/** @topos: collapse boundary (∂Φ failure states as values) */
sealed interface PhaseCollapse {
    data class Overload(val phaseName: String, val load: Double) : PhaseCollapse
    data class ResonanceLost(val phaseName: String) : PhaseCollapse
    data class Interference(val reason: String) : PhaseCollapse
}

/** @topos: Φ (immutable phase space / state carrier) */
data class ToposSpace(
    val activePhases: Set<String> = emptySet(),
    val phaseDepth: Int = 0,
    val tension: Double = 1.0,
    val trace: List<String> = emptyList()
) {
    /** @transition: Φ → Φ (phase activation) */
    fun addPhase(name: String) = copy(activePhases = activePhases + name)

    /** @transition: Φ → Φ (tension accumulation) */
    fun amplify(amount: Double) = copy(tension = tension + amount)

    /** @transition: Φ → Φ (trace projection) */
    fun recordTrace(event: String) = copy(trace = trace + event)

    /** @transition: Φ → Φ (phase progression depth) */
    fun tickResonance() = copy(phaseDepth = phaseDepth + 1)
}

/**
 * @topos: ∂Φ (phase propagation boundary)
 * Ψ stream → Φ′ evaluation → Φ evolution | collapse
 */
object LoopJunction {
    private val log = LoggerFactory.getLogger(javaClass)

    fun <T> Flow<T>.propagate(
        initialSpace: ToposSpace,
        transition: suspend Raise<PhaseCollapse>.(state: ToposSpace, psi: T) -> ToposSpace
    ): Flow<Either<PhaseCollapse, ToposSpace>> = flow {

        /** @state: Φ₀ (initial topology) */
        var currentSpace = initialSpace

        /** @phase: Ψ ingress → Φ′ evaluation loop */
        this@propagate.collect { ex ->

            /** @phase: Φ′ (evaluation with collapse short-circuit) */
            val xe = either { transition(currentSpace, ex) }

            when (xe) {
                is Either.Right -> {
                    /** @transition: Φ → Φ (state evolution) */
                    currentSpace = xe.value

                    log.trace("[Φ] advanced to depth {}", currentSpace.phaseDepth)

                    /** @emit: Φ → Ψ′ (projection outward) */
                    emit(xe)
                }

                is Either.Left -> {
                    /** @collapse: ∂Φ breach (topology broken) */
                    val collapse = xe.value

                    log.warn("[∂Φ] collapse detected: {}", collapse)

                    /** @emit: collapse projection */
                    emit(xe)

                    /** @termination: stop Ψ ingestion */
                    return@collect
                }
            }
        }
    }
}

suspend fun exLoop(exStream: Flow<PsiEvent>) {

    /** @init: Φ₀ (base topology state) */
    val initialSpace = ToposSpace(tension = 1.0)

    exStream

        // @phase.1: Ψ ingress (external signal flow)

        // @phase.2: ∂Φ boundary → Φ′ evaluation → Φ evolution
        .propagate(initialSpace) { state, ex ->

            /** @guard: ∂Φ overflow (tension breach) */
            if (state.tension > 10.0) {
                raise(Overload(phaseName = "P_r", load = state.tension))
            }

            /** @guard: ∂Φ invalid topology (psi malformed) */
            if (ex.payload == "MALFORMED") {
                raise(Interference("Invalid topological structure"))
            }

            /** @transition: Φ → Φ (phase update) */
            state.addPhase(ex.kind)
                .amplify(0.5)
                .tickResonance()
                .recordTrace("Psi ingested: ${ex.kind}")
        }

        // @phase.3: Ψ′ projection (result handling / re-entry)
        .collect { current ->
            current.fold(
                ifLeft = { collapse ->
                    /** @reentry: collapse → Ψ′ (error phase emission) */
                    println("Trigger re-entry logic due to $collapse")
                },
                ifRight = { space ->
                    /** @observe: Φ stabilization trace */
                    println("Loop propagate tension: ${space.tension}")
                }
            )
        }
}