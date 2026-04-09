package subst.bound.emitter

import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import subst.bound.psi.PsiEvent

@Component
class Emitter(
    private val redis: ReactiveRedisTemplate<String, String>
) {

    suspend fun emit(psi: PsiEvent) {
        val key = psi.tag
        val kind = psi.kind

        if (kind.contains("removed")) {
            redis.delete(key).awaitFirstOrNull()
        } else {
            // @step.1: 상태 보존 (Grounding)
            redis.opsForValue().set(key, "1").awaitFirstOrNull()

            // @step.2: 파동 반향 (Echoing - Perturbator가 관측)
            val echoChannel = "${key.substringBefore(":")}:echo"
            redis.convertAndSend(echoChannel, "resonance:${kind}").awaitFirstOrNull()
        }
    }
}