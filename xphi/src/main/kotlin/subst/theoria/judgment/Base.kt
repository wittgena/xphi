package subst.theoria.judgment

@DslMarker
annotation class JudgmentDsl

sealed interface PhiElement { val name: String }
data class Psi(override val name: String) : PhiElement
data class Phi(override val name: String) : PhiElement
data class Bound(override val name: String) : PhiElement

// AST (No execution logic allowed)
sealed interface PhiDecl
data class StepDecl(val name: String, val body: List<PhiDecl>)
data class FlowDecl(val from: Psi, val to: Phi, val bound: Bound) : PhiDecl
data class CollapseDecl(val sources: List<Psi>, val bound: Phi) : PhiDecl
data class PhiJudgment(val steps: List<StepDecl>)

class TheoriaBuilder {
    private val steps = mutableListOf<StepDecl>()

    fun step(name: String, block: StepBuilder.() -> Unit) {
        val builder = StepBuilder(name)
        builder.block()
        steps += builder.build()
    }

    fun build(): PhiJudgment {
        return PhiJudgment(steps.toList())
    }
}

class StepBuilder(private val name: String) {
    private val body = mutableListOf<PhiDecl>()

    fun flow(expr: FlowExpr) {
        body += FlowDecl(expr.from, expr.to, expr.bound)
    }

    fun collapse(from: List<Psi>, into: Phi) {
        body += CollapseDecl(from, into)
    }

    fun build(): StepDecl {
        return StepDecl(name, body.toList())
    }
}

data class FlowExpr(val from: Psi, val to: Phi, val bound: Bound)
infix fun Pair<Psi, Phi>.at(bound: Bound) = FlowExpr(first, second, bound)
fun psi(name: String) = Psi(name)
fun phi(name: String) = Phi(name)
fun bound(name: String) = Bound(name)

// Entry Point
fun theoria(block: TheoriaBuilder.() -> Unit): PhiJudgment {
    val builder = TheoriaBuilder()
    builder.block()
    return builder.build()
}

