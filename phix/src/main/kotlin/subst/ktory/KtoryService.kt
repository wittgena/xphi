package subst.ktory

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.DisposableBean
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicReference

@Service
class KtoryService : DisposableBean {

    private val engineRef = AtomicReference<KtoryEngine>()

    init {
        engineRef.set(KtoryEngine())
    }

    private fun engine(): KtoryEngine =
        engineRef.get() ?: error("KtoryEngine not initialized")

    fun analyzeStream(path: String): Flow<KotlinExContract> {
        return engine().analyzeStream(path)
    }

    suspend fun analyzeSource(content: String): KotlinExContract =
        withContext(Dispatchers.Default) {
            engine().analyzeSource("inline.kt", content)
        }

    fun reload() {
        val newEngine = KtoryEngine()
        val old = engineRef.getAndSet(newEngine)
        old?.dispose()
    }

    override fun destroy() {
        engineRef.get()?.dispose()
    }
}