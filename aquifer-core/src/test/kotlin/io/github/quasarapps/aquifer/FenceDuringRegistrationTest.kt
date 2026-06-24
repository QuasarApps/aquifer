package io.github.quasarapps.aquifer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A mutation that races the *registration* of a fetch — landing after the fetch has taken its
 * single-flight slot but before the fetch has captured its fencing epoch — must still fence the
 * fetch off.
 *
 * This is the register-then-fence window: the fetch's epoch is captured at registration, not in
 * the lazily-started fetch body, so a `put`/`invalidate` in that gap can never let the fetch's
 * commit clobber the write that fenced it. [MutationFencingTest] only covers fences against a
 * fetch whose body is already running (epoch already captured); this pins the earlier window.
 */
class FenceDuringRegistrationTest {

    /**
     * Runs dispatched work only when explicitly [drain]ed, so the fetch body — dispatched here
     * by `scope.async(LAZY).start()` — can be held queued while a `put` lands on another
     * dispatcher, reproducing the register-then-fence interleaving deterministically.
     */
    private class ManualDispatcher : CoroutineDispatcher() {
        private val queue = ArrayDeque<Runnable>()

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            queue.addLast(block)
        }

        fun drain() {
            while (queue.isNotEmpty()) queue.removeFirst().run()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `a put racing fetch registration is not overwritten when the fetch resolves`() = runTest {
        val fetchDispatcher = ManualDispatcher()
        val fetchScope = CoroutineScope(SupervisorJob() + fetchDispatcher)
        val store = aquifer<String, String> {
            scope(fetchScope)
            fetcher { "from-network" }
        }

        // get() registers the single-flight fetch and dispatches its body to fetchDispatcher,
        // where it sits queued and unrun — so the only fencing epoch that exists is the one
        // captured at registration.
        val getter = launch { store.get("k", Freshness.NetworkOnly) }
        try {
            runCurrent() // run get() up to awaiting the fetch; the body is queued on fetchDispatcher

            // The local write lands in the registration→body gap. With the epoch captured at
            // registration it fences the fetch; with the epoch captured in the (still-queued)
            // body the fence would be missed and the fetch below would clobber this write.
            store.put("k", "local")
            runCurrent()

            // Now let the queued fetch body run to completion: it resolves "from-network" for its
            // awaiting caller, but its commit is fenced off, so the cache keeps the local write.
            fetchDispatcher.drain()
            runCurrent()

            assertEquals("local", store.get("k", Freshness.CacheOnly))
        } finally {
            // Always tear down, even if an assertion above fails, so no work leaks into later
            // tests: cancel/join the getter before cancelling the scope it fetched in.
            getter.cancelAndJoin()
            fetchScope.cancel()
        }
    }
}
