package subst.bound.psi.strategy

class SlidingWindowStrategy(
    private val windowMillis: Long,
    private val stepMillis: Long
) : WindowStrategy {
    override fun generate(
        trajectory: ContinuousTrajectory
    ): List<WindowedTrajectory> {
        val points = trajectory.points
        if (points.isEmpty()) return emptyList()

        val windows = mutableListOf<WindowedTrajectory>()
        var current = points.first().timestamp
        val end = points.last().timestamp

        while (current + windowMillis <= end) {
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
            current += stepMillis
        }
        return windows
    }
}