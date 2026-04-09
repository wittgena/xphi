package subst.bound.psi.strategy

class FixedWindowStrategy(
    private val windowMillis: Long
) : WindowStrategy {

    override fun generate(
        trajectory: ContinuousTrajectory
    ): List<WindowedTrajectory> {
        val points = trajectory.points
        if (points.isEmpty()) return emptyList()

        val windows = mutableListOf<WindowedTrajectory>()
        var current = points.first().timestamp
        val end = points.last().timestamp

        while (current < end) {
            val next = current + windowMillis
            val segment = points.filter {
                it.timestamp in current until next
            }
            windows.add(
                WindowedTrajectory(
                    trajectory.identity,
                    current,
                    next,
                    segment
                )
            )
            current = next
        }
        return windows
    }
}