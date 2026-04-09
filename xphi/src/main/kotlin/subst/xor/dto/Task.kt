package subst.xor.dto

import subst.xor.Block

// Redis Queue에 넣을 작업 단위
data class IndexTask(
    val jobId: String,
    val filePaths: List<String> // 쪼개진 파일 경로들
)

// 파싱 완료 후 Main에게 돌려줄 결과
data class IndexTaskResult(
    val jobId: String,
    val blocks: List<Block>,
    val filePaths: List<String> = emptyList()
)