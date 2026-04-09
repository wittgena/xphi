package subst.bound

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Component

@Component
class SnowflakeIdGene(
    private val redis: ReactiveStringRedisTemplate,
    @Value("\${xphi.project-code:XOR-ALPHA}") private val projectCode: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // Snowflake 알고리즘 파라미터
    private val twepoch = 1672531200000L // 기준 시간 (2023-01-01)
    private val workerIdBits = 10L // 최대 1024대의 노드 구동 가능
    private val maxWorkerId = -1L xor (-1L shl workerIdBits.toInt())
    private val sequenceBits = 12L // 밀리초당 최대 4096개 ID 생성 가능

    private val workerIdShift = sequenceBits
    private val timestampLeftShift = sequenceBits + workerIdBits
    private val sequenceMask = -1L xor (-1L shl sequenceBits.toInt())

    private var workerId: Long = 0
    private var sequence = 0L
    private var lastTimestamp = -1L

    @PostConstruct
    fun initWorkerId() {
        // Redis INCR을 통해 분산 환경에서도 겹치지 않는 고유 노드 ID 획득
        val redisKey = "xphi:$projectCode:worker_seq"

        // PostConstruct 초기화 단계이므로 안전하게 block() 사용
        val id = redis.opsForValue().increment(redisKey).block() ?: 1L

        // 1024를 넘어가면 다시 0부터 순환하도록 처리
        this.workerId = id % (maxWorkerId + 1)
        log.info("[Snowflake] Initialized with Dynamic Worker ID: $workerId")
    }

    @Synchronized
    fun nextId(): Long {
        var timestamp = timeGen()

        if (timestamp < lastTimestamp) {
            throw RuntimeException("Clock moved backwards. Refusing to generate id.")
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) and sequenceMask
            if (sequence == 0L) {
                timestamp = tilNextMillis(lastTimestamp)
            }
        } else {
            sequence = 0L
        }

        lastTimestamp = timestamp

        // Snowflake ID 비트 연산 조립
        return ((timestamp - twepoch) shl timestampLeftShift.toInt()) or
                (workerId shl workerIdShift.toInt()) or
                sequence
    }

    // 현재 노드의 고유 ID를 문자열 접두어 형태로 반환 (선택 사항)
    fun getNodeId(): String = "node-$workerId"

    private fun tilNextMillis(lastTimestamp: Long): Long {
        var timestamp = timeGen()
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen()
        }
        return timestamp
    }

    private fun timeGen(): Long = System.currentTimeMillis()
}