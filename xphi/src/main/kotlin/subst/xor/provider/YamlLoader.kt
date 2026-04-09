package subst.xor.provider

import org.yaml.snakeyaml.Yaml
import java.nio.file.Files
import java.nio.file.Path
import java.net.URI

object YamlLoader {

    fun load(cliPath: String? = null): AppConfig {
        val configPath = resolveConfigPath(cliPath)
        require(Files.exists(configPath)) {
            "config file not found: $configPath"
        }

        val yaml = Yaml()
        val map = Files.newInputStream(configPath).use {
            yaml.load<Map<String, Any>>(it)
        } ?: throw IllegalStateException("config.yaml is empty")

        val indexMap = requireSection(map, "index")
        val searchMap = requireSection(map, "search")
        val blocksMap = requireSection(map, "blocks")

        val resolvedIndexPath = resolvePath(indexMap.requireString("path"))
        val resolvedBlocksRoot = resolvePath(blocksMap.requireString("root"))

        return AppConfig(
            index = IndexConfig(
                path = resolvedIndexPath,
                analyzer = indexMap.getOrDefault("analyzer", "standard") as String,
                ramBufferMB = (indexMap.getOrDefault("ramBufferMB", 128) as Number).toDouble(),
                openMode = indexMap.getOrDefault("openMode", "CREATE") as String
            ),
            search = SearchConfig(
                defaultField = searchMap.getOrDefault("defaultField", "content") as String,
                topK = (searchMap.getOrDefault("topK", 20) as Number).toInt()
            ),
            blocks = BlocksConfig(
                root = resolvedBlocksRoot,
                recursive = blocksMap.getOrDefault("recursive", true) as Boolean
            )
        )
    }

    // @config.path.resolution
    private fun resolveConfigPath(cliPath: String?): Path {

        // @step.1: CLI override
        if (cliPath != null) {
            return Path.of(cliPath).toAbsolutePath().normalize()
        }

        // @step.2: ENV override
        System.getenv("XOR_CONFIG")?.let {
            return Path.of(it).toAbsolutePath().normalize()
        }

        // @step.3: self root 기준 (.anchor/xor.yaml)
        findSelfRoot()?.let { selfRoot ->
            val selfConfig = selfRoot.resolve(".anchor/xor.yaml")
            if (Files.exists(selfConfig)) {
                return selfConfig
            }
        }

        // 4. jar 위치 기준
        runCatching {
            val jarDir = Path.of(
                URI.create(
                    YamlLoader::class.java
                        .protectionDomain
                        .codeSource
                        .location
                        .toURI()
                        .toString()
                )
            ).parent

            val jarConfig = jarDir.resolve("config.yaml")
            if (Files.exists(jarConfig)) {
                return jarConfig
            }
        }

        // 5. CWD fallback
        val cwdConfig = Path.of("config.yaml")
        if (Files.exists(cwdConfig)) {
            return cwdConfig.toAbsolutePath().normalize()
        }

        throw IllegalStateException("No config.yaml found (CLI, ENV, self, jar, cwd)")
    }

    // SELF ROOT 탐지 (단순 구현)
    private fun findSelfRoot(start: Path = Path.of("").toAbsolutePath()): Path? {

        var current: Path? = start

        while (current != null) {
            val metaDir = current.resolve(".meta")
            if (Files.exists(metaDir)) {
                return current
            }
            current = current.parent
        }

        return null
    }

    // YAML UTIL
    private fun requireSection(
        root: Map<String, Any>,
        key: String
    ): Map<String, Any> {

        val section = root[key]
            ?: throw IllegalArgumentException("Missing required section: $key")

        require(section is Map<*, *>) {
            "Section '$key' must be a map"
        }

        @Suppress("UNCHECKED_CAST")
        return section as Map<String, Any>
    }

    private fun Map<String, Any>.requireString(key: String): String {
        val value = this[key]
            ?: throw IllegalArgumentException("Missing required key: $key")

        require(value is String) {
            "Key '$key' must be a string"
        }

        return value
    }

    private fun resolveEnv(value: String): String {
        val regex = Regex("\\$\\{([^}:]+)(:([^}]+))?}")
        return regex.replace(value) { match ->
            val varName = match.groupValues[1]
            val defaultValue = match.groupValues.getOrNull(3)

            System.getenv(varName)
                ?: defaultValue
                ?: throw IllegalArgumentException(
                    "Environment variable '$varName' is not set and no default provided."
                )
        }
    }

    private fun resolvePath(raw: String): String {
        var resolved = resolveEnv(raw)
        if (resolved.startsWith("~")) {
            val home = System.getProperty("user.home")
            resolved = resolved.replaceFirst("~", home)
        }

        val path = Path.of(resolved)
        return if (path.isAbsolute) {
            path.normalize().toString()
        } else {
            path.toAbsolutePath().normalize().toString()
        }
    }
}