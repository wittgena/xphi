package subst.theoria.judgment

import subst.bound.psi.PsiEvent
import subst.theoria.phi.executor.FlowExecutor
import subst.theoria.phi.runtime.PhiInterpreter
import subst.theoria.phi.runtime.PhiRuntimeContext

object Interaction {
    val meta = theoria {
        step("interaction") {
            flow(
                psi("alpha") to phi("beta") at bound("γ")
            )
            collapse(
                from = listOf(psi("beta")),
                into = phi("omega")
            )
        }
    }

    fun flowHandler(
        executor: FlowExecutor
    ): suspend (PsiEvent) -> Unit = { psi ->

        executor.execute(meta)
            .collect {
                // 필요 시 Dispatcher로 재주입 가능
                // 또는 로깅/중계
            }
    }

    fun judgmentHandler(
        interpreter: PhiInterpreter,
        runtimeContext: PhiRuntimeContext
    ): suspend (PsiEvent) -> Unit = { psi ->

        interpreter.run(meta, runtimeContext)
    }
}
