package subst.theoria.phi.runtime

import org.springframework.stereotype.Component
import subst.bound.psi.Namespace
import subst.bound.psi.Router
import subst.bound.psi.PsiEvent

@Component
class Receptor(
    private val router: Router
) {
    private var phase = Namespace.Phase.IDLE

    suspend fun process(psi: PsiEvent) {
        val route = router.route(psi)
        if (phase == Namespace.Phase.IDLE) {
            phase = route.targetPhase
        }
        route.handler(psi)
    }
}