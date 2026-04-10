
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.*
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import subst.xor.Block
import subst.xor.analyzer.UnderscoreAwareAnalyzer
import java.io.Closeable
import java.nio.file.Path

data class XorResult(
    val score: Float,
    val blockId: String?,
    val blockType: String?,
    val sectionPath: String?,
    val filePath: String?
)

class XorEngine(
    indexPath: Path,
    private val ramBufferMB: Double = 256.0
) : Closeable {

    private val directory: Directory = FSDirectory.open(indexPath)
    private val analyzer = UnderscoreAwareAnalyzer()

    /**
     * [Index] 메모리 상의 Block 리스트를 Lucene 인덱스로 플러시
     */
    fun indexBlocks(blocks: List<Block>, append: Boolean = false) {
        val openMode = if (append) IndexWriterConfig.OpenMode.CREATE_OR_APPEND
        else IndexWriterConfig.OpenMode.CREATE

        val writerConfig = IndexWriterConfig(analyzer).apply {
            this.openMode = openMode
            this.ramBufferSizeMB = ramBufferMB
        }

        IndexWriter(directory, writerConfig).use { writer ->
            blocks.forEach { block ->
                val doc = Document().apply {
                    // identity
                    add(StringField("block_id", block.block_id, Field.Store.YES))
                    add(StringField("file_path", block.file_path, Field.Store.YES))
                    add(StringField("source_type", block.source_type, Field.Store.YES))

                    // structural
                    add(StringField("section", block.section, Field.Store.YES))
                    add(StringField("section_path", block.section_path, Field.Store.YES))
                    add(IntPoint("section_depth", block.section_depth))
                    add(StoredField("section_depth_store", block.section_depth))
                    add(StringField("block_type", block.block_type, Field.Store.YES))

                    // 주의: 원본 코드에서 meta 값 null 처리 보완 (Elvis 연산자 활용)
                    add(StringField("meta", block.meta ?: "", Field.Store.NO))
                    add(IntPoint("order_index", block.order_index))
                    add(StoredField("order_index_store", block.order_index))

                    // symbols
                    block.symbols.forEach {
                        add(StringField("symbol", it, Field.Store.NO))
                    }

                    // content
                    add(TextField("content", block.content ?: "", Field.Store.NO))
                    add(TextField("dsl_name", block.dsl_name ?: "", Field.Store.NO))
                }
                writer.addDocument(doc)
            }
            writer.commit()
        }
    }

    /**
     * [Search] 쿼리 문자열을 파싱하여 정형화된 검색 결과 객체 리스트를 반환
     */
    fun search(queryStr: String, blockType: String? = null, topK: Int = 10): List<XorResult> {
        // @future: NRT(Near Real-Time) 검색을 고도화하려면 DirectoryReader를 캐싱검토
        if (!DirectoryReader.indexExists(directory)) return emptyList()

        return DirectoryReader.open(directory).use { reader ->
            val searcher = IndexSearcher(reader)
            val fields = arrayOf("content", "section_path", "symbol")
            val boosts = mapOf(
                "section_path" to 2.0f,
                "symbol" to 1.5f,
                "content" to 1.0f
            )

            val parser = MultiFieldQueryParser(fields, analyzer, boosts)
            parser.defaultOperator = QueryParser.Operator.AND

            val mainQuery = parser.parse(queryStr)
            val finalQuery = BooleanQuery.Builder()
                .add(mainQuery, BooleanClause.Occur.MUST)
                .apply {
                    if (!blockType.isNullOrBlank()) {
                        add(TermQuery(Term("block_type", blockType)), BooleanClause.Occur.FILTER)
                    }
                }.build()

            val topDocs = searcher.search(finalQuery, topK)
            topDocs.scoreDocs.map { scoreDoc ->
                val doc = searcher.doc(scoreDoc.doc)
                XorResult(
                    score = scoreDoc.score,
                    blockId = doc.get("block_id"),
                    blockType = doc.get("block_type"),
                    sectionPath = doc.get("section_path"),
                    filePath = doc.get("file_path")
                )
            }
        }
    }

    override fun close() {
        directory.close()
    }
}