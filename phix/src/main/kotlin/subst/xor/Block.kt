package subst.xor

data class Block(
    val block_id: String,
    val file_path: String,
    val source_type: String,
    val section: String,
    val section_path: String,
    val section_depth: Int,
    val block_type: String,
    val meta: String? = null,
    val order_index: Int,
    val symbols: List<String>,
    val content: String?,
    val dsl_name: String? = null
)
