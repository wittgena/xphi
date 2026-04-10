package subst.bound.psi

import subst.bound.psi.strategy.ContinuousTrajectory
import subst.bound.psi.strategy.PsiPoint
import subst.bound.psi.strategy.WindowStrategy

class PsiAttenuator(
    private val strategy: WindowStrategy,
    private val emitter: suspend (PsiEvent) -> Unit
) {
    private val buffer = mutableListOf<PsiPoint>()

    suspend fun handle(psi: PsiEvent) {
        buffer.add(
            PsiPoint(
                timestamp = System.currentTimeMillis(),
                psi = psi
            )
        )

        val trajectory = ContinuousTrajectory(
            identity = psi.tag,
            points = buffer.toList()
        )

        val windows = strategy.generate(trajectory)
        if (windows.isNotEmpty()) {
            val latest = windows.last()
            if (latest.points.isNotEmpty()) {
                emitter(latest.points.last().psi)
                buffer.clear()
            }
        }
    }
}