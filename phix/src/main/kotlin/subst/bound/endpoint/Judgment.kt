package subst.bound.endpoint

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import subst.theoria.phi.executor.FlowExecutor
import subst.theoria.phi.runtime.PhiInterpreter
import subst.theoria.phi.runtime.PhiRuntimeContext
import subst.theoria.Loader
import subst.theoria.judgment.PhiJudgment

@RestController
@RequestMapping("/judgment")
class JudgmentController(
    private val loader: Loader,
    private val executor: FlowExecutor,
    private val interpreter: PhiInterpreter
) {
    private val log = LoggerFactory.getLogger(javaClass)

    data class JudgmentResponse(
        val status: String,
        val result: String,
        val contextTrace: Map<String, Any>
    )

    /**
     * Reentry 판단 요청 API
     * POST /judgment/reentry
     */
    @PostMapping("/reentry")
    suspend fun evaluateReentry(@RequestBody body: String): JudgmentResponse {
        // 1. 요청마다 독립적인 런타임 컨텍스트 생성 (스레드 안전성 확보)
        val requestContext = PhiRuntimeContext()

        // 2. 컴파일 및 로드 작업을 I/O 스레드로 오프로드하여 Event Loop 블로킹 방지
        val meta = withContext(Dispatchers.IO) {
            load(body)
        }

        try {
            // 3. Flow 비동기 실행 (필요시 context 전달)
            executor.execute(meta).collect { flowEvent ->
                log.debug("[Reentry Flow] Event: {}", flowEvent)
            }

            // 4. Judgment 루프 실행 및 결과 획득
            val judgmentResult = interpreter.run(meta, requestContext)

            // 5. 실행 후 컨텍스트에서 외부에 노출할 메타데이터 추출 (옵션)
            val traceData = mapOf(
                "lastFlowFrom" to (requestContext["lastFlow.from"] ?: "unknown"),
                "collapsed" to (requestContext["collapsed"] ?: false)
            )

            return JudgmentResponse(
                status = "SUCCESS",
                result = judgmentResult.javaClass.simpleName, // Term, Collapse, Drift 등
                contextTrace = traceData
            )

        } catch (e: Exception) {
            log.error("[Reentry Error] Failed to evaluate judgment", e)
            return JudgmentResponse(
                status = "ERROR",
                result = e.message ?: "Unknown Error",
                contextTrace = emptyMap()
            )
        }
    }

    private fun load(body: String): PhiJudgment {
        return if (body.contains("```")) {
            loader.loadFromString(body)
        } else {
            loader.loadDsl(body)
        }
    }
}