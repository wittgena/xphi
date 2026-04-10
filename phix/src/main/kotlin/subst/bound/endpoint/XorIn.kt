package subst.bound.endpoint

import XorResult
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.Flow
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import subst.xor.service.DistService
import subst.xor.service.XorService
import java.nio.file.Path

@RestController
@RequestMapping("/xor")
class XorIn(
    private val xe: XorService,
    private val dist: DistService
) {
    private val logger = KotlinLogging.logger {}

    @PostMapping("/reload")
    fun reload(@RequestParam(required = false) path: String?): String {
        xe.reload(path)
        return "Reloaded successfully at ${System.currentTimeMillis()}"
    }

    @PostMapping("/index", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun index(@RequestParam(required = false) path: String?): Flow<String> {
        val targetPath = path ?: xe.getCurrentConfig().blocks.root
        logger.info { "@request -> Method: POST, Path: /xor/index, Param: $path" }
        return xe.indexFromFilesStream(Path.of(targetPath))
    }

    @PostMapping("/index-dist", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    suspend fun indexDist(@RequestParam(required = false) path: String?): Flow<String> {
        val targetPath = path ?: xe.getCurrentConfig().blocks.root
        logger.info { "@request-dist -> Method: POST, Path: /xor/index-dist, Param: $path" }
        return dist.distributeAndIndex(Path.of(targetPath))
    }

    @GetMapping("/search", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun search(
        @RequestParam query: String,
        @RequestParam(required = false) type: String?,
        @RequestParam(required = false) topK: Int?
    ): Flow<XorResult> {
        return xe.searchStream(query, type, topK)
    }
}