package subst.theoria

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.kotlin.cli.jvm.K2JVMCompiler
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.net.URLClassLoader

object DslExtractor {
    private val pattern = Regex(
        "```kotlin([\\s\\S]*?)```",
        RegexOption.MULTILINE
    )

    fun extract(md: String): String {
        val match = pattern.find(md)
            ?: error("DSL block not found")

        return match.groupValues[1].trim()
    }
}

object DslTemplate {
    fun wrap(dsl: String): String = """
        package generated.theoria

        import kotlin.*
        import kotlin.Pair
        import subst.theoria.judgment.*
        import subst.bound.psi.*

        object DynamicInteraction {
            val meta = $dsl
        }
    """.trimIndent()
}

@Component
class KtCompiler {
    private val log = KotlinLogging.logger {}

    fun compile(code: String, fqcn: String): Class<*> {
        val tempDir = createTempDir()
        val source = File(tempDir, "DynamicFlow.kt")
        source.writeText(code)

        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()

        val compiler = K2JVMCompiler()
        val classpath = System.getProperty("java.class.path")
        log.info { " [Diagnostic] Compiling FQCN: $fqcn" }

        val result = compiler.exec(
            PrintStream(out),
            "-cp", classpath, // 클래스패스 옵션 추가
            "-d", tempDir.absolutePath,
            source.absolutePath
        )

        if (result.code != 0) {
            val compilerLogs = out.toString()

            // 콘솔에 직접 출력 (로그 레벨에 상관없이 보이도록)
            System.err.println("\n--- [KOTLIN COMPILE ERROR] ---")
            System.err.println(compilerLogs)
            System.err.println("--- [GENERATED SOURCE] ---")
            System.err.println(code)
            System.err.println("------------------------------\n")

            throw IllegalStateException("DSL Compile Failed. Check console logs for details.")
        }

        val classLoader = URLClassLoader(
            arrayOf(tempDir.toURI().toURL()),
            this::class.java.classLoader
        )

        return classLoader.loadClass(fqcn)
    }
}