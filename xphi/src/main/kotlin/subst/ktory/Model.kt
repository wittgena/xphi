package subst.ktory

data class ClassInfo(val name: String, val startLine: Int, val endLine: Int)
data class FunctionInfo(val name: String, val calls: List<String>, val startLine: Int, val endLine: Int)
data class LmpComment(val keyword: String, val content: String, val location: String)
data class DslNode(val phase: String, val label: String, val location: String)
data class DslExBundle(val function: String, val dslNodes: List<DslNode>, val calls: List<String>)
data class Contract(
    val kind: String,
    val name: String,
    val features: List<String>,
    val refs: List<String>,
    val location: String
)

data class KotlinExContract(
    val source: String,
    val facts: List<Contract>,
    val functions: List<FunctionInfo>,
    val classes: List<ClassInfo>,
    val annotations: List<String>,
    val lmpComments: List<LmpComment>,
    val dslNodes: List<DslNode>,
    val executionBundles: List<DslExBundle>
)
