package subst.xor.provider

import org.springframework.stereotype.Component
import org.yaml.snakeyaml.Yaml
import java.nio.file.Files
import java.nio.file.Path
import java.net.URI

@Component
class XorConfigProvider(
    private val baseConfig: AppConfig
) {
    fun refreshConfig(cliPath: String? = null): AppConfig {
        val configPath = resolveConfigPath(cliPath)

        // 외부 설정 파일이 없으면 application.yaml 기반의 기본값을 그대로 반환
        if (configPath == null || !Files.exists(configPath)) {
            println("No external config found. Using application.yaml defaults.")
            return baseConfig
        }

        println("Reloading Xor config from: $configPath")
        return parseAndMerge(configPath)
    }

    private fun parseAndMerge(configPath: Path): AppConfig {
        val yaml = Yaml()
        val map = Files.newInputStream(configPath).use {
            yaml.load<Map<String, Any>>(it)
        } ?: return baseConfig

        // [수정1] subst.xor 계층 구조 지원 (application.yaml과 동일한 트리 구조 대응)
        val substMap = map["subst"] as? Map<String, Any> ?: emptyMap()
        val xorMap = substMap["xor"] as? Map<String, Any> ?: map // subst.xor가 없으면 루트 map으로 fallback

        val indexMap = xorMap["index"] as? Map<String, Any> ?: emptyMap()
        val searchMap = xorMap["search"] as? Map<String, Any> ?: emptyMap()
        val blocksMap = xorMap["blocks"] as? Map<String, Any> ?: emptyMap()

        val schemaMap = indexMap["blockSchema"] as? Map<String, Any> ?: emptyMap()
        @Suppress("UNCHECKED_CAST")
        val blockSchema = BlockSchemaConfig(
            identityFields = (schemaMap["identityFields"] as? List<String>) ?: baseConfig.index.blockSchema.identityFields,
            keywordFields = (schemaMap["keywordFields"] as? List<String>) ?: baseConfig.index.blockSchema.keywordFields,
            textFields = (schemaMap["textFields"] as? List<String>) ?: baseConfig.index.blockSchema.textFields
        )

        return AppConfig(
            index = IndexConfig(
                // [수정2] baseConfig로 fallback 할 때도 무조건 resolvePath를 통과하도록 괄호 위치 변경
                path = resolvePath(indexMap["path"]?.toString() ?: baseConfig.index.path),
                analyzer = indexMap["analyzer"]?.toString() ?: baseConfig.index.analyzer,
                ramBufferMB = (indexMap["ramBufferMB"] as? Number)?.toDouble() ?: baseConfig.index.ramBufferMB,
                openMode = indexMap["openMode"]?.toString() ?: baseConfig.index.openMode,
                blockSchema = blockSchema
            ),
            search = SearchConfig(
                defaultField = searchMap["defaultField"]?.toString() ?: baseConfig.search.defaultField,
                topK = (searchMap["topK"] as? Number)?.toInt() ?: baseConfig.search.topK
            ),
            blocks = BlocksConfig(
                // [수정2] blocks.root 역시 baseConfig fallback 시 resolvePath 적용
                root = resolvePath(blocksMap["root"]?.toString() ?: baseConfig.blocks.root),
                recursive = (blocksMap["includeSubDirs"] as? Boolean)
                    ?: (blocksMap["recursive"] as? Boolean)
                    ?: baseConfig.blocks.recursive
            )
        )
    }

    // @config.path.resolution
    private fun resolveConfigPath(cliPath: String?): Path? {
        // @step.1: CLI override
        if (cliPath != null) return Path.of(cliPath).toAbsolutePath().normalize()

        // @step.2: ENV override
        System.getenv("XOR_CONFIG")?.let { return Path.of(it).toAbsolutePath().normalize() }

        // @step.3: self root 기준 (.anchor/xor.yaml)
        findSelfRoot()?.let { selfRoot ->
            val selfConfig = selfRoot.resolve(".anchor/xor.yaml")
            if (Files.exists(selfConfig)) return selfConfig
        }

        // 4. jar 위치 기준 (YamlLoader 잔재 제거 및 현재 클래스 참조로 변경)
        runCatching {
            val jarDir = Path.of(
                URI.create(
                    this::class.java.protectionDomain.codeSource.location.toURI().toString()
                )
            ).parent
            val jarConfig = jarDir.resolve("config.yaml")
            if (Files.exists(jarConfig)) return jarConfig
        }

        // 5. CWD fallback
        val cwdConfig = Path.of("config.yaml")
        if (Files.exists(cwdConfig)) return cwdConfig.toAbsolutePath().normalize()

        // 못 찾으면 throw 대신 null 반환 -> application.yaml로 fallback 되도록 유도
        return null
    }

    private fun findSelfRoot(start: Path = Path.of("").toAbsolutePath()): Path? {
        var current: Path? = start
        while (current != null) {
            // yaml에 명시된 .anchor 기준으로 탐색
            val anchorDir = current.resolve(".anchor")
            if (Files.exists(anchorDir)) return current
            current = current.parent
        }
        return null
    }

    private fun resolveEnv(value: String): String {
        val regex = Regex("\\$\\{([^}:]+)(:([^}]+))?}")
        return regex.replace(value) { match ->
            val varName = match.groupValues[1]

            // [수정3] groupValues 대신 groups를 사용하여 실제로 default 값 그룹이 존재하는지 체크
            val defaultGroup = match.groups[3]

            System.getenv(varName)
                ?: defaultGroup?.value
                ?: throw IllegalArgumentException("Environment variable '$varName' is not set.")
        }
    }

    private fun resolvePath(raw: String): String {
        var resolved = resolveEnv(raw)
        if (resolved.startsWith("~")) {
            val home = System.getProperty("user.home")
            resolved = resolved.replaceFirst("~", home)
        }
        val path = Path.of(resolved)
        return if (path.isAbsolute) path.normalize().toString()
        else path.toAbsolutePath().normalize().toString()
    }
}