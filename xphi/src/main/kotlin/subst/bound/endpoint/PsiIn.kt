package subst.bound.endpoint
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import subst.bound.psi.PsiEvent
import subst.theoria.phi.runtime.Dispatcher
import subst.bound.SnowflakeIdGene

@RestController
class PsiIn(
    private val dispatcher: Dispatcher,
    private val snowflake: SnowflakeIdGene
) {

    /**
     * External Ψ ingress
     *
     * - JSON → PsiEvent 정규화
     * - Dispatcher로 단일 진입
     */
    @PostMapping("/psi")
    suspend fun emit(@RequestBody body: String): String {
        val event = try {
            PsiEvent.fromJson(body)
        } catch (e: Exception) {
            PsiEvent.createFallback(
                channel = "api:psi",
                rawPayload = body,
                snowflakeId = snowflake.nextId().toString(),
                sourceId = snowflake.getNodeId()
            )
        }
        dispatcher.dispatch(event)
        return "accepted"
    }
}