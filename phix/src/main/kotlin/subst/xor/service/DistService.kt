package subst.xor.service

import XorEngine
import reactor.util.retry.Retry
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.scheduler.Schedulers
import subst.xor.Block
import subst.xor.dto.IndexTask
import subst.xor.dto.IndexTaskResult
import subst.xor.provider.XorConfigProvider
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@Service
class DistService(
    private val redis: ReactiveStringRedisTemplate,
    private val configProvider: XorConfigProvider,
    private val mapper: ObjectMapper
): DisposableBean {
    private val log = LoggerFactory.getLogger(DistService::class.java)

    private val TASK_QUEUE = "xor:index:tasks"
    private val engineRef = AtomicReference<XorEngine>()

    @PostConstruct
    fun startWorkerLoop() {
        log.info("[Worker] 워커 루프 초기화 시작")
        val config = configProvider.refreshConfig()
        engineRef.set(XorEngine(Path.of(config.index.path)))
        startConsumingTasks()
        log.info("[Worker] 워커 루프 초기화 완료, Task 대기열 리스닝 시작")
    }

    suspend fun distributeAndIndex(blockRoot: Path): Flow<String> = callbackFlow {
        val jobId = UUID.randomUUID().toString()
        log.info("[Main-$jobId] 분산 인덱싱 작업 시작. 경로: {}", blockRoot)
        trySend("jobId: $jobId")

        // 확장자 필터 조건 (기존과 동일)
        val allowedExt = setOf("md", "kt", "json", "yaml")
        val allFiles = Files.walk(blockRoot)
            .filter { path ->
                val ext = path.fileName.toString().substringAfterLast('.', "").lowercase()
                ext in allowedExt
            }
            .toList()

        if (allFiles.isEmpty()) {
            log.warn("[Main-$jobId] 처리할 파일이 없습니다. Flow를 종료합니다.")
            trySend("No files to process.")
            close()
            return@callbackFlow
        }

        val chunks = allFiles.chunked(50)
        val expectedChunks = chunks.size
        log.info("[Main-$jobId] 총 {}개 파일 발견. {}개의 청크로 분할됨.", allFiles.size, expectedChunks)

        // 스트림 출력을 알림
        trySend("Starting index for root: $blockRoot")

        // 동시성 제어를 위한 변수들
        val receivedChunks = AtomicInteger(0)
        val totalProcessedFiles = AtomicInteger(0) // 누적 처리 파일 수
        val totalAggregatedBlocks = AtomicInteger(0) // 누적 처리 블록 수
        val aggregatedBlocks = ConcurrentLinkedQueue<Block>()

        val resultChannel = "xor:result:$jobId"
        log.info("[Main-$jobId] Pub/Sub 채널 구독 준비: {}", resultChannel)

        // 1. 결과 구독 설정 (작업 밀어넣기 전)
        val subscription = redis.listenToChannel(resultChannel)
            .doOnSubscribe { log.info("[Main-$jobId] Pub/Sub 채널 구독 완료 (대기 시작)") }
            .subscribe(
                { message ->
                    try {
                        val result = mapper.readValue<IndexTaskResult>(message.message)
                        aggregatedBlocks.addAll(result.blocks)

                        val currentReceived = receivedChunks.incrementAndGet()
                        val currentBlocks = totalAggregatedBlocks.addAndGet(result.blocks.size)

                        log.info("[Main-$jobId] 결과 수신됨 (진행률: {}/{}), 청크 블록 수: {}", currentReceived, expectedChunks, result.blocks.size)

                        // 2. 워커로부터 받은 filePaths를 순회하며 개별 스트림 로그 발송
                        result.filePaths.forEach { filePath ->
                            val currentFiles = totalProcessedFiles.incrementAndGet()
                            val fileName = Path.of(filePath).fileName.toString()

                            // 로컬 인덱싱과 동일한 형태의 로그 전송
                            trySend("Indexed $fileName ($currentBlocks blocks in $currentFiles files)")
                        }

                        // 3. 모든 청크가 도착했을 때의 처리
                        if (currentReceived == expectedChunks) {
                            log.info("[Main-$jobId] 모든 청크 수신 완료. 최종 인덱싱 시작.")
                            val engine = engineRef.get()
                            engine.indexBlocks(aggregatedBlocks.toList(), append = true)

                            log.info("[Main-$jobId] 최종 인덱싱 완료. 총 블록 수: {}", aggregatedBlocks.size)
                            trySend("Indexing complete. Total: ${aggregatedBlocks.size} blocks.")
                            close()
                        }
                    } catch (e: Exception) {
                        log.error("[Main-$jobId] 결과 수신 중 에러 발생: {}", e.message, e)
                    }
                },
                { err -> log.error("[Main-$jobId] Pub/Sub 리스너 에러: {}", err.message, err) }
            )

        // 4. 구독 설정 완료 후 작업을 Queue에 밀어넣기
        log.info("[Main-$jobId] 워커 큐(TASK_QUEUE)에 {}개 작업 Push 시작", expectedChunks)
        chunks.forEachIndexed { index, chunk ->
            val task = IndexTask(jobId, chunk.map { it.toString() })
            redis.opsForList().rightPush(TASK_QUEUE, mapper.writeValueAsString(task))
                .subscribe(
                    { log.debug("[Main-$jobId] 청크 {}/{} Push 성공", index + 1, expectedChunks) },
                    { err -> log.error("[Main-$jobId] 청크 {}/{} Push 실패: {}", index + 1, expectedChunks, err.message) }
                )
        }
        log.info("[Main-$jobId] 워커 큐 Push 완료")

        // 5. 종료 시 정리
        awaitClose {
            log.info("[Main-$jobId] 작업 종료로 인한 리소스 및 구독 정리")
            subscription.dispose()
        }
    }

    private fun startConsumingTasks() {
        redis.opsForList().leftPop(TASK_QUEUE, Duration.ofSeconds(1))
            .repeat()
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(10))
                .doBeforeRetry { retrySignal ->
                    log.warn("[Worker] Redis 연결 끊김 감지. 재시도 중... (누적: ${retrySignal.totalRetries()})")
                }
            )
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                { taskJson ->
                    try {
                        val task = mapper.readValue<IndexTask>(taskJson)
                        log.info("[Worker] 작업 획득: jobId={}, 파일 개수={}", task.jobId, task.filePaths.size)

                        val parsedBlocks = parseFilesToBlocks(task.filePaths)
                        log.info("[Worker] 파싱 완료: jobId={}, 생성된 블록 수={}", task.jobId, parsedBlocks.size)

                        // 주의: IndexTaskResult가 filePaths를 받도록 DTO 수정이 선행되어야 합니다.
                        val result = IndexTaskResult(task.jobId, parsedBlocks, task.filePaths)

                        // 결과 발송
                        redis.convertAndSend("xor:result:${task.jobId}", mapper.writeValueAsString(result))
                            .subscribe(
                                { log.info("[Worker] 결과 발송 완료: jobId={}", task.jobId) },
                                { err -> log.error("[Worker] 결과 발송 실패: jobId={}, 에러={}", task.jobId, err.message) }
                            )
                    } catch (e: Exception) {
                        log.error("[Worker] 작업 처리 중 에러 발생", e)
                    }
                },
                { err -> log.error("[Worker] 큐 리스닝 루프 에러", err) }
            )
    }

    private fun parseFilesToBlocks(paths: List<String>): List<Block> {
        return paths.flatMap { path ->
            val json = Files.readString(Path.of(path))
            mapper.readValue<List<Block>>(json)
        }
    }

    override fun destroy() {
        log.info("DistService 파괴됨, 워커 정리 필요 시 이 곳에 구현")
    }
}