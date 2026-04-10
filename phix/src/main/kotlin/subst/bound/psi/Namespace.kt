package subst.bound.psi

import org.springframework.stereotype.Component

@Component
class Namespace {
    enum class Phase {
        IDLE,
        ACTIVE_PSI,
        FLOW,
        JUDGMENT
    }

    enum class PhiDomain {
        PSI,
        FLOW,
        LOOP,
        XSEARCH,
        WATCHER
    }

    data class PhaseRoute(
        val domain: PhiDomain,
        val targetPhase: Phase,
        val handler: suspend (PsiEvent) -> Unit
    )

    private val routes = mutableMapOf<String, PhaseRoute>()

    fun register(prefix: String, route: PhaseRoute) {
        routes[prefix] = route
    }

    fun resolve(tag: String): PhaseRoute {
        val prefix = tag.substringBefore(":")
        return routes[prefix] ?: error("Unknown namespace: $prefix")
    }
}
