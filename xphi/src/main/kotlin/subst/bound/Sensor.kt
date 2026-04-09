import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.boot.web.context.WebServerInitializedEvent
import org.springframework.context.event.EventListener
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import subst.bound.SnowflakeIdGene
import subst.bound.psi.PsiEvent
import subst.theoria.phi.runtime.Bootstrap
import subst.theoria.phi.runtime.Dispatcher

@Component
class Sensor(
    private val redis: ReactiveRedisTemplate<String, String>,
    private val bootstrap: Bootstrap,
    private val snowflake: SnowflakeIdGene,
    private val dispatcher: Dispatcher
) {
    private val log = LoggerFactory.getLogger(Sensor::class.java)
    private val mapper = jacksonObjectMapper()
    private var apiBase = ""

    @EventListener
    fun onWebServerReady(event: WebServerInitializedEvent) {
        val actualPort = event.webServer.port
        this.apiBase = "http://127.0.0.1:$actualPort/xor"
        log.info("[Sensor] Node awakened on Dynamic Port: $actualPort. apiBase ready.")
    }

    @PostConstruct
    fun startListening() {
        // Echolocation
        redis.listenToChannel("system:ping").subscribe { _ ->
            val echoPayload = mapOf("status" to "ALIVE", "api_base" to apiBase)
            redis.convertAndSend(
                "system:echo",
                mapper.writeValueAsString(echoPayload)
            ).subscribe()
        }

        // Perturbation 수신
        redis.listenToPattern("*:intensity").subscribe { message ->
            val channel = message.channel
            val payloadJson = message.message

            bootstrap.poke()
            val event = try {
                PsiEvent.fromJson(payloadJson)
            } catch (e: Exception) {
                PsiEvent.createFallback(
                    channel = channel,
                    rawPayload = payloadJson,
                    snowflakeId = snowflake.nextId().toString(),
                    sourceId = snowflake.getNodeId()
                )
            }

            log.debug("[Sensor] Ψ detected: ${event.carrier.tag}")
            CoroutineScope(Dispatchers.Default).launch {
                dispatcher.dispatch(event)
            }
        }
    }
}