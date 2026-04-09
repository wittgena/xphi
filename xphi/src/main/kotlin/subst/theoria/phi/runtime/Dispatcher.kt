package subst.theoria.phi.runtime

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.springframework.stereotype.Component
import subst.bound.psi.PsiEvent

@Component
class Dispatcher(
    private val machine: Receptor
) {

    private val queue = Channel<PsiEvent>(capacity = 512)

    init {
        CoroutineScope(Dispatchers.Default).launch {
            for (psi in queue) {
                machine.process(psi)
            }
        }
    }

    suspend fun dispatch(psi: PsiEvent) {
        queue.send(psi)
    }
}