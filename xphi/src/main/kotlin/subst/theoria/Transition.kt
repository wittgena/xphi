package subst.theoria

interface PhiStructure

/**
 * Phi 전이 결과 모델
 * - Transited: 구조 전이 성공
 * - Drift: 위상 불일치 (구조 정렬 실패)
 */
sealed class PhiTransition<I : PhiStructure, O : PhiStructure> {
    data class Transited<I : PhiStructure, O : PhiStructure>(
        val value: O
    ) : PhiTransition<I, O>()

    data class Drift<I : PhiStructure, O : PhiStructure>(
        val original: I,
        val reason: String? = null
    ) : PhiTransition<I, O>()
}

