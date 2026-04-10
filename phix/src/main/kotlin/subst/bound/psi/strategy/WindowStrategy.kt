package subst.bound.psi.strategy

import subst.bound.psi.PsiEvent

interface WindowStrategy {
    fun generate(
        trajectory: ContinuousTrajectory
    ): List<WindowedTrajectory>
}

data class PsiPoint(
    val timestamp: Long,
    val psi: PsiEvent
)

data class ContinuousTrajectory(
    val identity: String,
    val points: List<PsiPoint>
)

data class WindowedTrajectory(
    val identity: String,
    val startTime: Long,
    val endTime: Long,
    val points: List<PsiPoint>
)