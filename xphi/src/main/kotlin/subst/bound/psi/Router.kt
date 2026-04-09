package subst.bound.psi

import org.springframework.stereotype.Component

@Component
class Router (
    val namespace: Namespace
) {
    fun route(psi: PsiEvent): Namespace.PhaseRoute {
        return namespace.resolve(psi.tag)
    }
}