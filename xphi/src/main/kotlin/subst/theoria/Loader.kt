package subst.theoria

import org.springframework.stereotype.Component
import subst.theoria.judgment.PhiJudgment
import java.io.File

@Component
class Loader(
    private val compiler: KtCompiler
) {
    private val FQCN = "theoria.proto.flow"

    fun load(path: String): PhiJudgment {
        val content = read(path)
        val dsl = try {
            if (content.contains("```")) {
                DslExtractor.extract(content)
            } else {
                content
            }
        } catch (e: Exception) {
            throw IllegalStateException(
                """
                DSL extract failed
                
                [raw]
                ${content.take(1000)}
                """.trimIndent(),
                e
            )
        }

        val code = try {
            DslTemplate.wrap(dsl)
        } catch (e: Exception) {
            throw IllegalStateException(
                """
                Template wrap failed
                
                [dsl]
                ${dsl.take(1000)}
                """.trimIndent(),
                e
            )
        }

        val clazz = try {
            compiler.compile(code, FQCN)
        } catch (e: Exception) {
            throw IllegalStateException(
                """
                Compile failed
                
                [dsl]
                ${dsl.take(1000)}
                
                [wrapped code]
                ${code.take(1000)}
                """.trimIndent(),
                e
            )
        }

        return try {
            val instance = clazz.getField("INSTANCE").get(null)
            val method = clazz.getDeclaredMethod("getMeta")
            method.invoke(instance) as PhiJudgment
        } catch (e: Exception) {
            throw IllegalStateException(
                """
                Reflection failed
                
                [class] ${clazz.name}
                """.trimIndent(),
                e
            )
        }
    }

    private fun read(path: String): String {

        // 1. classpath 우선
        val resource = this::class.java.classLoader.getResource(path)
        if (resource != null) {
            return resource.readText()
        }

        // 2. fallback: filesystem
        val file = File(path)
        if (file.exists()) {
            return file.readText()
        }

        error("DSL not found: $path")
    }

    fun loadFromString(md: String): PhiJudgment {
        val dsl = DslExtractor.extract(md)
        return compile(dsl)
    }

    fun loadDsl(dsl: String): PhiJudgment {
        return compile(dsl)
    }

    private fun compile(dsl: String): PhiJudgment {
        val code = DslTemplate.wrap(dsl)
        val clazz = compiler.compile(code, FQCN)
        val instance = clazz.getField("INSTANCE").get(null)
        val method = clazz.getDeclaredMethod("getMeta")
        return method.invoke(instance) as PhiJudgment
    }


}