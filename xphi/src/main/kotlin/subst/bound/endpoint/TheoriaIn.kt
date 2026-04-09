package subst.bound.endpoint

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import subst.theoria.phi.executor.FlowExecutor
import subst.theoria.phi.runtime.PhiInterpreter
import subst.theoria.phi.runtime.PhiRuntimeContext
import subst.theoria.Loader
import subst.theoria.judgment.PhiJudgment

@RestController
class TheoriaIn(
    private val loader: Loader,
    private val executor: FlowExecutor,
    private val interpreter: PhiInterpreter
) {

    private val runtime = PhiRuntimeContext()

    /**
     * DSL ingress (markdown or raw theoria)
     *
     * - md or kotlin DSL 모두 허용
     * - 즉시 compile → execute
     */
    @PostMapping("/theoria")
    suspend fun run(@RequestBody body: String): String {
        val meta = load(body)

        // 1. flow 실행
        executor.execute(meta).collect {
            // 필요시 Dispatcher 재주입 가능
        }

        // 2. judgment 실행 (선택)
        interpreter.run(meta, runtime)
        return "executed"
    }

    private fun load(body: String): PhiJudgment {
        return if (body.contains("```")) {
            loader.loadFromString(body)   // markdown
        } else {
            loader.loadDsl(body)         // raw DSL
        }
    }
}