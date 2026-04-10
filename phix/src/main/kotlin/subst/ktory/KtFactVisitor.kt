package subst.ktory

import org.jetbrains.kotlin.com.intellij.openapi.util.text.StringUtil
import org.jetbrains.kotlin.com.intellij.psi.PsiComment
import org.jetbrains.kotlin.psi.*

data class VisitorContext(
    val fileText: String,
    var currentContext: String = "<global>",
    val functions: MutableList<FunctionInfo> = mutableListOf(),
    val classes: MutableList<ClassInfo> = mutableListOf(),
    val annotations: MutableSet<String> = mutableSetOf(),
    val lmpComments: MutableList<LmpComment> = mutableListOf()
)

class KtFactVisitor(
    private val ctx: VisitorContext
) : KtTreeVisitorVoid() {
    private fun getLineRange(element: KtElement): Pair<Int, Int> {
        val startOffset = element.textRange.startOffset
        val endOffset = element.textRange.endOffset
        val startLine = StringUtil.offsetToLineNumber(ctx.fileText, startOffset) + 1
        val endLine = StringUtil.offsetToLineNumber(ctx.fileText, endOffset) + 1
        return Pair(startLine, endLine)
    }

    override fun visitNamedFunction(function: KtNamedFunction) {
        val name = function.name ?: "<anonymous>"
        ctx.currentContext = name

        val calls = mutableListOf<String>()
        function.accept(object : KtTreeVisitorVoid() {
            override fun visitCallExpression(expression: KtCallExpression) {
                expression.calleeExpression?.text?.let { calls += it }
                super.visitCallExpression(expression)
            }
        })

        val (startLine, endLine) = getLineRange(function)
        ctx.functions += FunctionInfo(name, calls, startLine, endLine)
        function.annotationEntries.forEach { ctx.annotations += it.text }

        super.visitNamedFunction(function)
    }

    override fun visitClass(klass: KtClass) {
        val name = klass.name ?: "<anonymous>"
        ctx.currentContext = name

        val (startLine, endLine) = getLineRange(klass)
        ctx.classes += ClassInfo(name, startLine, endLine)
        klass.annotationEntries.forEach { ctx.annotations += it.text }
        super.visitClass(klass)
    }

    override fun visitComment(comment: PsiComment) {
        val match =
            Regex("@([a-zA-Z]+(?:\\.[a-zA-Z]+)?):\\s*(.+)")
                .find(comment.text.trim())

        match?.let {
            val (keyword, content) = it.destructured
            ctx.lmpComments += LmpComment(
                keyword = "@$keyword",
                content = content,
                location = ctx.currentContext
            )

        }
        super.visitComment(comment)
    }
}
