package subst.bound.endpoint

import com.google.gson.GsonBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import subst.ktory.*

@RestController
@RequestMapping("/ktory")
class KtoryEndpoint(
    private val service: KtoryService
) {
    private val gson = GsonBuilder().setPrettyPrinting().create()

    @PostMapping("/analyze", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun analyze(@RequestParam path: String): Flow<String> {
        return service.analyzeStream(path)
            .map { "data: ${gson.toJson(it)}\n\n" }
    }

    @PostMapping("/analyze-text")
    suspend fun analyzeText(@RequestBody content: String): String {
        val result = service.analyzeSource(content)
        return gson.toJson(result)
    }
}

