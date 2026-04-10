package subst.theoria

import org.springframework.stereotype.Component
import subst.bound.psi.Namespace
import subst.theoria.phi.executor.FlowExecutor
import subst.theoria.phi.runtime.PhiInterpreter
import subst.theoria.phi.runtime.PhiRuntimeContext

@Component
class Binder(
    namespace: Namespace,
    executor: FlowExecutor,
    interpreter: PhiInterpreter,
    loader: Loader
) {

    private val runtime = PhiRuntimeContext()
//    init {
//        val meta = kotlin.runCatching {
//            loader.load("bind.phix")
//        }.getOrElse {
//            error("Failed to load DSL: ${it.message}")
//        }
//
//        // FLOW
//        namespace.register(
//            prefix = "interaction.flow",
//            route = Namespace.PhaseRoute(
//                domain = Namespace.PhiDomain.FLOW,
//                targetPhase = Namespace.Phase.FLOW,
//                handler = { psi ->
//                    executor.execute(meta).collect { }
//                }
//            )
//        )
//
//        // JUDGMENT
//        namespace.register(
//            prefix = "interaction.judgment",
//            route = Namespace.PhaseRoute(
//                domain = Namespace.PhiDomain.PSI,
//                targetPhase = Namespace.Phase.JUDGMENT,
//                handler = { psi ->
//                    interpreter.run(meta, runtime)
//                }
//            )
//        )
//    }
}