package subst.theoria.judgment

data class Judgment(val phases: List<PhaseDecl>)
data class PhaseDecl(
    val name: String,
    val bind: BindDecl?,
    val flow: List<FlowBlock>
) : PhiDecl
data class BindDecl(val nodes: List<String>, val edges: List<String>)

interface FlowBlock : PhiDecl
data class LoopDecl(val name: String, val blocks: List<FlowBlock>) : FlowBlock
data class CascadeDecl(
    val name: String,
    val stages: List<PathDecl>,
    val result: List<ActionDecl>
) : FlowBlock
data class PathDecl(val sequence: List<PhiElement>) : FlowBlock
data class InputDecl(val psi: Psi) : FlowBlock
data class InterfereDecl(val operations: List<InterfereOp>) : FlowBlock
data class ReflectDecl(val conditions: List<ConditionBlock>) : FlowBlock

sealed interface InterfereOp
data class AlignOp(val left: PhiElement, val right: PhiElement) : InterfereOp
data class PhaseShiftOp(
    val left: PhiElement,
    val right: PhiElement,
    val range: ClosedFloatingPointRange<Double>
) : InterfereOp

// Conditions
sealed interface ConditionBlock
data class IfAligned(val actions: List<ActionDecl>) : ConditionBlock
data class IfEqual(val left: PhiElement, val right: PhiElement, val actions: List<ActionDecl>) : ConditionBlock
data class ElseBlock(val actions: List<ActionDecl>) : ConditionBlock

// Actions
sealed interface ActionDecl
data class EmitDecl(val expr: Expr) : ActionDecl
data class AbsorbDecl(val target: PhiElement) : ActionDecl
data class InjectDecl(val target: String) : ActionDecl
object GainIncreaseDecl : ActionDecl

sealed interface Expr
data class Ref(val name: String) : Expr
data class Call(val target: Expr, val method: String, val args: List<Expr> = emptyList()) : Expr

@JudgmentDsl
class PhaseBuilder(private val name: String) {
    private var bind: BindDecl? = null
    private val flow = mutableListOf<FlowBlock>()
    fun bind(block: BindBuilder.() -> Unit) {
        val b = BindBuilder()
        b.block()
        bind = b.build()
    }

    fun flow(block: FlowBuilder.() -> Unit) {
        val fb = FlowBuilder()
        fb.block()
        flow += fb.build()
    }

    fun build(): PhaseDecl = PhaseDecl(name, bind, flow)
}

class BindBuilder {
    private val nodes = mutableListOf<String>()
    private val edges = mutableListOf<String>()
    fun node(name: String) { nodes += name }
    fun edge(name: String) { edges += name }
    fun build() = BindDecl(nodes, edges)
}

@JudgmentDsl
class FlowBuilder {
    private val blocks = mutableListOf<FlowBlock>()
    fun loop(name: String, block: LoopBuilder.() -> Unit) {
        val b = LoopBuilder(name)
        b.block()
        blocks += b.build()
    }

    fun cascade(name: String, block: CascadeBuilder.() -> Unit) {
        val b = CascadeBuilder(name)
        b.block()
        blocks += b.build()
    }

    fun build(): List<FlowBlock> = blocks
}

class LoopBuilder(private val name: String) {
    private val blocks = mutableListOf<FlowBlock>()
    fun input(p: Psi) { blocks += InputDecl(p) }
    fun interfere(block: InterfereBuilder.() -> Unit) {
        val b = InterfereBuilder()
        b.block()
        blocks += b.build()
    }

    fun reflect(block: ReflectBuilder.() -> Unit) {
        val b = ReflectBuilder()
        b.block()
        blocks += b.build()
    }

    fun build(): LoopDecl = LoopDecl(name, blocks)
}

class CascadeBuilder(private val name: String) {
    private val stages = mutableListOf<PathDecl>()
    private val result = mutableListOf<ActionDecl>()

    fun stage(block: PathBuilder.() -> Unit) {
        val b = PathBuilder()
        b.block()
        stages += b.build()
    }

    fun result(block: ActionBuilder.() -> Unit) {
        val b = ActionBuilder()
        b.block()
        result += b.build()
    }

    fun build(): CascadeDecl = CascadeDecl(name, stages, result)
}

@JudgmentDsl
class PathBuilder {
    private val sequence = mutableListOf<PhiElement>()

    // 체이닝을 지원하는 중위 연산자 (예: a leadsTo b leadsTo c)
    infix fun PhiElement.leadsTo(other: PhiElement): PhiElement {
        if (sequence.isEmpty()) sequence.add(this)
        if (sequence.last() != this) sequence.add(this) // 중복 방지
        sequence.add(other)
        return other
    }

    // 명시적 가변 인자 방식
    fun path(vararg elems: PhiElement) { sequence.addAll(elems) }
    fun build(): PathDecl = PathDecl(sequence)
}

class InputBuilder {
    private lateinit var psi: Psi
    fun psi(p: Psi) { psi = p }
    fun build(): InputDecl = InputDecl(psi)
}

class InterfereBuilder {
    private val ops = mutableListOf<InterfereOp>()
    fun align(a: PhiElement, b: PhiElement) { ops += AlignOp(a, b) }
    fun phaseShift(a: PhiElement, b: PhiElement, range: ClosedFloatingPointRange<Double>) {
        ops += PhaseShiftOp(a, b, range)
    }

    fun build(): InterfereDecl = InterfereDecl(ops)
}

@JudgmentDsl
class ReflectBuilder {
    private val conditions = mutableListOf<ConditionBlock>()

    fun ifEqual(a: PhiElement, b: PhiElement, block: ActionBuilder.() -> Unit): ElseContext {
        val ab = ActionBuilder().apply(block)
        conditions += IfEqual(a, b, ab.build())
        return ElseContext() // 체이닝을 위한 컨텍스트 객체 반환
    }

    inner class ElseContext {
        infix fun otherwise(block: ActionBuilder.() -> Unit) {
            val ab = ActionBuilder().apply(block)
            conditions += ElseBlock(ab.build())
        }
    }

    fun build(): ReflectDecl = ReflectDecl(conditions)
}

class ActionBuilder {
    private val actions = mutableListOf<ActionDecl>()
    fun emit(expr: Expr) { actions += EmitDecl(expr) }
    fun emit(name: String) { actions += EmitDecl(Ref(name)) }
    fun absorb(target: PhiElement) { actions += AbsorbDecl(target) }
    fun inject(target: String) { actions += InjectDecl(target) }
    fun gainIncrease() { actions += GainIncreaseDecl }
    fun build(): List<ActionDecl> = actions
}
