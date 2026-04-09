package subst.xor.service

import XorEngine
import XorResult
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.DisposableBean
import org.springframework.stereotype.Service
import subst.xor.Block
import subst.xor.provider.AppConfig
import subst.xor.provider.XorConfigProvider
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import kotlin.io.path.readText

@Service
class XorService(
    private val configProvider: XorConfigProvider
) : DisposableBean {
    private val logger = KotlinLogging.logger {}
    private val engineRef = AtomicReference<XorEngine>()
    private var currentConfig: AppConfig = configProvider.refreshConfig()
    private val mapper = jacksonObjectMapper()

    init {
        XorEngine(
            indexPath = Path.of(currentConfig.index.path),
            ramBufferMB = currentConfig.index.ramBufferMB
        ).let {
            engineRef.set(it)
        }
    }

    fun getCurrentConfig(): AppConfig = currentConfig

    fun reload(cliPath: String?) {
        val newConfig = configProvider.refreshConfig(cliPath)
        val newEngine = XorEngine(
            indexPath = Path.of(newConfig.index.path),
            ramBufferMB = newConfig.index.ramBufferMB
        )

        val oldEngine = engineRef.getAndSet(newEngine)
        this.currentConfig = newConfig

        oldEngine?.close()
    }

    /**
     * 지정된 루트 경로의 JSON 파일들을 읽어 인덱싱합니다.
     * [개선점] 파일 단위 I/O 대신 벌크 인서트로 변경하여 성능 극대화
     */
    suspend fun indexFromFiles(blockRoot: Path) = withContext(Dispatchers.IO) {
        val engine = engineRef.get() ?: throw IllegalStateException("Engine not initialized")
        val allBlocks = mutableListOf<Block>() // 파싱된 블록들을 모아둘 메모리 버퍼

        // Files.walk의 스트림을 안전하게 닫기 위해 use 블록 사용
        Files.walk(blockRoot).use { stream ->
            stream.filter { Files.isRegularFile(it) }
                .filter { it.toString().endsWith(".json") }
                .forEach { jsonPath ->
                    val json = jsonPath.readText()
                    val blocks: List<Block> = mapper.readValue(json)
                    allBlocks.addAll(blocks) // 엔진에 바로 넣지 않고 버퍼에 담기
                }
        }

        // 루프가 끝난 뒤 모아둔 블록을 엔진에 단 1번만 Bulk Insert
        if (allBlocks.isNotEmpty()) {
            engine.indexBlocks(allBlocks, append = true)
        }
    }

    /**
     * 실시간 인덱싱 프로그레스 스트림
     * [개선점] 스트림 로그는 파일 단위로 쏴주되, 실제 엔진 인덱싱은 맨 마지막에 한 번에 처리
     */
    fun indexFromFilesStream(blockRoot: Path): Flow<String> = flow {
        val engine = engineRef.get() ?: throw IllegalStateException("Engine not initialized")
        emit("Starting index for root: $blockRoot")

        var fileCount = 0
        var blockCount = 0
        val allBlocks = mutableListOf<Block>() // 벌크 인서트를 위한 컬렉션 추가

        logger.info { "Starting index for root: $blockRoot, fileCount: $fileCount, blockCount: $blockCount" }

        Files.walk(blockRoot).use { stream ->
            for (jsonPath in stream) {
                if (!Files.isRegularFile(jsonPath) || !jsonPath.toString().endsWith(".json")) continue

                val json = jsonPath.readText()
                val blocks: List<Block> = mapper.readValue(json)

                // 파일별로 파싱한 데이터를 엔진에 즉시 넣지 않고 컬렉션에 누적합니다.
                allBlocks.addAll(blocks)

                fileCount++
                blockCount += blocks.size

                val msg = "Parsed ${jsonPath.fileName}"
                logger.info { "@action: $msg" }

                // 프론트엔드/CLI에 파일별 파싱 완료 진행상황을 즉시 전송
                emit("Indexed ${jsonPath.fileName} ($blockCount blocks in $fileCount files)")
            }
        }

        // 💥 엔진에 실제 쓰기(Write) 작업을 수행하는 구간 (단 1번 실행)
        if (allBlocks.isNotEmpty()) {
            emit("Saving ${allBlocks.size} blocks to search engine (Bulk Write)...")
            engine.indexBlocks(allBlocks, append = true)
        }

        emit("Indexing complete. Total: $blockCount blocks.")
        logger.info { "Indexing complete. Total: $blockCount blocks." }
    }.flowOn(Dispatchers.IO)

    suspend fun search(query: String, type: String?, topK: Int? = null): List<XorResult> = withContext(Dispatchers.IO) {
        val engine = engineRef.get() ?: throw IllegalStateException("Engine not initialized")
        val limit = topK ?: currentConfig.search.topK
        return@withContext engine.search(query, type, limit)
    }

    fun searchStream(query: String, type: String?, topK: Int?): Flow<XorResult> = flow {
        val engine = engineRef.get() ?: throw IllegalStateException("Engine not initialized")
        val limit = topK ?: currentConfig.search.topK

        val results = engine.search(query, type, limit)
        results.forEach { emit(it) }
    }.flowOn(Dispatchers.IO)

    override fun destroy() {
        engineRef.get()?.close()
    }
}