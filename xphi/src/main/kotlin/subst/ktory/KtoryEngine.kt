package subst.ktory

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.jetbrains.kotlin.cli.common.CLIConfigurationKeys
import org.jetbrains.kotlin.cli.jvm.compiler.EnvironmentConfigFiles
import org.jetbrains.kotlin.cli.jvm.compiler.KotlinCoreEnvironment
import org.jetbrains.kotlin.com.intellij.openapi.Disposable
import org.jetbrains.kotlin.com.intellij.openapi.util.Disposer
import org.jetbrains.kotlin.config.CommonConfigurationKeys
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.psi.KtPsiFactory
import java.io.File

class KtoryEngine : Disposable {
    private val disposable = Disposer.newDisposable()
    private val psiFactory: KtPsiFactory

    init {
        // 1. 컴파일러 환경 설정 (최초 1회만 실행)
        val configuration = CompilerConfiguration().apply {
            put(CommonConfigurationKeys.MODULE_NAME, "ktory-module")
            put(
                CLIConfigurationKeys.MESSAGE_COLLECTOR_KEY,
                org.jetbrains.kotlin.cli.common.messages.MessageCollector.NONE
            )
        }

        val environment = KotlinCoreEnvironment.createForProduction(
            disposable,
            configuration,
            EnvironmentConfigFiles.JVM_CONFIG_FILES
        )
        this.psiFactory = KtPsiFactory(environment.project)
    }

    fun analyzeStream(path: String): Flow<KotlinExContract> = flow {
        val target = File(path)
        if (!target.exists()) return@flow

        val ktFiles = collectKtFiles(target)
        for (file in ktFiles) {
            emit(dissolve(file, psiFactory))
        }
    }

    fun analyzeSource(fileName: String, content: String): KotlinExContract {
        return dissolve(File(fileName), psiFactory, contentOverride = content)
    }

    override fun dispose() {
        Disposer.dispose(disposable)
    }

    private fun collectKtFiles(target: File): List<File> =
        // Input normalization
        when {
            target.isFile && target.extension == "kt" -> listOf(target)
            target.isDirectory ->
                target.walkTopDown()
                    .filter { it.isFile && it.extension == "kt" }
                    .toList()

            else -> emptyList()
        }

    private fun dissolve(
        file: File,
        psiFactory: KtPsiFactory,
        contentOverride: String? = null // 추가: 메모리 직접 주입용
    ): KotlinExContract {
        val fileText = contentOverride ?: file.readText()
        val ktFile = psiFactory.createFile(file.name, fileText)
        val ctx = VisitorContext(fileText)

        ktFile.accept(KtFactVisitor(ctx))
        val dslNodes = convertLmpToDsl(ctx.lmpComments)
        val executionBundles = groupDslByFunction(dslNodes, ctx.functions)
        val facts = toContract(ctx.functions, ctx.classes, ctx.annotations)

        return KotlinExContract(
            source = file.path,
            facts = facts,
            functions = ctx.functions,
            classes = ctx.classes,
            annotations = ctx.annotations.toList(),
            lmpComments = ctx.lmpComments,
            dslNodes = dslNodes,
            executionBundles = executionBundles
        )
    }
}