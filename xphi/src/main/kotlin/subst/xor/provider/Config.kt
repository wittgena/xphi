package subst.xor.provider

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "subst.xor")
data class AppConfig(
    val index: IndexConfig,
    val search: SearchConfig,
    val blocks: BlocksConfig
)

data class IndexConfig(
    val path: String,
    val analyzer: String,
    val ramBufferMB: Double,
    val openMode: String,
    val blockSchema: BlockSchemaConfig = BlockSchemaConfig()
)

data class BlockSchemaConfig(
    val identityFields: List<String> = listOf("block_id", "file_path"),
    val keywordFields: List<String> = listOf("block_type", "section_path", "symbol"),
    val textFields: List<String> = listOf("content")
)

data class SearchConfig(val defaultField: String = "content", val topK: Int = 20)
data class BlocksConfig(val root: String, val recursive: Boolean = true)