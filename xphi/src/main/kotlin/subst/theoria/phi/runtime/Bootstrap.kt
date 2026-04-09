package subst.theoria.phi.runtime

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.*
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import subst.bound.SnowflakeIdGene
import subst.bound.psi.PsiAttenuator
import subst.bound.psi.strategy.SlidingWindowStrategy
import subst.bound.emitter.Emitter
import subst.bound.psi.Namespace
import subst.bound.psi.PsiCarrier
import subst.bound.psi.PsiEvent

@Component
class Bootstrap(
    private val namespace: Namespace,
    private val redis: ReactiveRedisTemplate<String, String>,
    private val emitter: Emitter,
    private val snowflake: SnowflakeIdGene,
) {
    private lateinit var attenuator: PsiAttenuator
    private val scope = CoroutineScope(Dispatchers.IO)

    @Volatile
    private var lastSeen: Long = System.currentTimeMillis()
    private val IDLE_TIMEOUT = 60_000L

    @PostConstruct
    fun init() {
        // 감쇠기: 핑퐁(Feedback Loop) 폭주를 막는 핵심 안전장치
        val strategy = SlidingWindowStrategy(windowMillis = 5_000, stepMillis = 1_000)
        attenuator = PsiAttenuator(strategy) { psi ->
            scope.launch { emitter.emit(psi) }
        }
        registerRoutes()
        announceAwakening() // 시스템 부트스트랩 시 활성화 펄스 발산
        startIdleWatcher()
    }

    fun poke() {
        lastSeen = System.currentTimeMillis()
    }

    fun receivePerturb(route: Namespace.PhaseRoute, event: PsiEvent) {
        scope.launch {
            route.handler.invoke(event)
        }
    }

    private fun announceAwakening() {
        // 노드가 깨어났음을 망(Network)에 알림
        scope.launch {
            // 수정됨: PsiEvent의 새로운 생성자 규격에 맞게 인자 전달
            val awakeEvent = PsiEvent(
                eventId = snowflake.nextId().toString(),
                parentId = null,
                sourceId = snowflake.getNodeId(),
                scope = "SYSTEM",
                tick = 0,
                carrier = PsiCarrier(
                    kind = "BOOTSTRAP",
                    tag = "system:awake",
                    payload = ""
                )
            )
            emitter.emit(awakeEvent)
        }
    }

    private fun registerRoutes() {
        namespace.register(
            "psi",
            Namespace.PhaseRoute(
                domain = Namespace.PhiDomain.PSI,
                targetPhase = Namespace.Phase.ACTIVE_PSI
            ) { psi ->
                attenuator.handle(psi)
                println("psi event ${psi.tag}")
            }
        )

        namespace.register(
            "execution",
            Namespace.PhaseRoute(
                domain = Namespace.PhiDomain.FLOW,
                targetPhase = Namespace.Phase.FLOW
            ) { psi ->
                attenuator.handle(psi)
                println("execution ${psi.tag}")
            }
        )
    }

    private fun startIdleWatcher() {
        scope.launch {
            while (isActive) {
                delay(5_000)

                val idle = System.currentTimeMillis() - lastSeen
                if (idle > IDLE_TIMEOUT) {
                    emitIdlePsi(idle)
                    requestReaper()
                    break
                }
            }
        }
    }

    private fun emitIdlePsi(duration: Long) {
        val event = PsiEvent(
            eventId = snowflake.nextId().toString(),
            parentId = null,
            sourceId = snowflake.getNodeId(),
            scope = "SYSTEM",
            tick = 0,
            carrier = PsiCarrier(
                kind = "TIMEOUT",
                tag = "system:idle",
                payload = "duration_ms=$duration"
            )
        )

        scope.launch { emitter.emit(event) }
    }

    private fun requestReaper() {
        val cmd = mapOf(
            "task" to "strike",
            "source_id" to snowflake.getNodeId()
        )
        redis.convertAndSend(
            "system:reaper:command",
            jacksonObjectMapper().writeValueAsString(cmd)
        ).subscribe()
    }
}