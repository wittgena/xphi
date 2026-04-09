package subst.theoria.flow

import org.slf4j.LoggerFactory

class TFlow {
    fun flow(name: String, block: FlowScope.() -> Unit) {
        val scope = FlowScope(name)
        scope.block()
        scope.execute()
    }
}

class FlowScope(private val name: String) {
    private val log = LoggerFactory.getLogger(javaClass)

    private var phiBlock: (() -> Unit)? = null
    private var interfereBlock: (() -> Unit)? = null
    private var reflectBlock: (() -> Unit)? = null
    private var convergeBlock: (() -> Unit)? = null

    fun phi(block: () -> Unit) {
        phiBlock = block
    }

    fun interfere(block: () -> Unit) {
        interfereBlock = block
    }

    fun reflect(block: () -> Unit) {
        reflectBlock = block
    }

    fun converge(block: () -> Unit) {
        convergeBlock = block
    }

    fun execute() {
        log.info("## tFlow: $name")

        phiBlock?.invoke()
        interfereBlock?.invoke()
        reflectBlock?.invoke()
        convergeBlock?.invoke()

        log.info("tFlow [$name] completed.")
    }
}

fun tFlow(block: TFlow.() -> Unit) {
    TFlow().block()
}