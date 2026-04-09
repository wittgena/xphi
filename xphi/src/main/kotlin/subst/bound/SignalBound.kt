package subst.bound

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.springframework.stereotype.Component
import subst.theoria.judgment.junction.LockJunction
import subst.theoria.judgment.junction.LockSignal
import subst.theoria.phi.runtime.PhiRuntimeContext

interface PhiStructure

data class LogSignal(val amplitude: Double) : PhiStructure

@Component
class SignalBound(
    private val junction: LockJunction
) {
    fun project(analog: LogSignal, name: String): LockSignal {
        return LockSignal(
            name = name,
            phase = analog.amplitude,
            amplitude = analog.amplitude,
            load = 0.1
        )
    }

    fun bind(
        scope: CoroutineScope,
        source: LogSignal,
        targetName: String,
        ctx: PhiRuntimeContext
    ): Job {
        val input = project(source, "src.$targetName")
        val output = LockSignal(targetName, 0.5, 0.5, 0.0)
        return junction.bind(
            scope = scope,
            input = input,
            output = output,
            onDrift = { drift -> ctx["signal.state"] = "drift" },
            onCollapse = { ctx["signal.state"] = "collapse" }
        ) { sig ->
            sig.copy(load = sig.load * 0.9)
        }
    }
}