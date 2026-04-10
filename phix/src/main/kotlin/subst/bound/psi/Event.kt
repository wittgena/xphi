package subst.bound.psi
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

data class PsiCarrier(
    val kind: String,
    val tag: String,
    // 파이썬은 payload: Any 이지만, 직렬화 시 주로 JSON 문자열을 쓰므로 String 유지
    val payload: String,

    // 파이썬의 Optional 동역학 필드들 (Minimal Alignment)
    @JsonProperty("carrier_type") val carrierType: String? = null,
    @JsonProperty("target_field") val targetField: String? = null,
    val temporal: String? = null,
    val spatial: String? = null,
    val persistence: String? = null
) {
    fun symbol(): String = "$kind:$tag"
}

data class PsiEvent(
    @JsonProperty("event_id") val eventId: String,
    @JsonProperty("parent_id") val parentId: String?,
    @JsonProperty("source_id") val sourceId: String,
    val scope: String,
    val tick: Int,
    val carrier: PsiCarrier,
    val context: Map<String, Any> = emptyMap(),
) {
    val tag: String get() = carrier.tag
    val kind: String get() = carrier.kind
    val payload: String get() = carrier.payload

    companion object {
        private val mapper: ObjectMapper = jacksonObjectMapper()

        // JSON 스트링에서 PsiEvent 객체로 역직렬화
        fun fromJson(json: String): PsiEvent {
            return mapper.readValue(json)
        }

        // 구형 Payload(단순 텍스트/이벤트)가 들어올 경우를 대비한 Fallback 생성기
        fun createFallback(channel: String, rawPayload: String, snowflakeId: String, sourceId: String): PsiEvent {
            return PsiEvent(
                eventId = snowflakeId, // UUID.randomUUID() 제거
                parentId = null,
                sourceId = sourceId,
                scope = "PHASE",
                tick = 0,
                carrier = PsiCarrier(
                    kind = "PERTURBED",
                    tag = channel,
                    payload = rawPayload
                )
            )
        }
    }
}