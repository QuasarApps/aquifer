package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.BatchKeyMissingException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration

/**
 * DataLoader-style coalescing: individual key [load]s that land within [window] of each other
 * are gathered and resolved by a single [fetch] call. Each `load` enqueues its key, suspends
 * on a shared slot, and is completed when the window's batch returns.
 *
 * - Same-key loads within one window share a slot (one result, one backend slot).
 * - The batch dispatches when the window elapses, or early once [maxBatchSize] distinct keys
 *   accumulate.
 * - A key the [fetch] omits fails only that key ([BatchKeyMissingException]); a throwing
 *   `fetch` fails every key in that batch (its callers' retry, if any, re-enqueues them).
 *
 * The mutex is held only for the O(1) enqueue/schedule bookkeeping — never across [fetch] or
 * an `await` — so a slow backend can't stall enqueuers. All scheduling runs in [scope]; under
 * virtual time the [window] delay is skipped while the test is idle, so a synchronous burst of
 * loads still coalesces.
 */
internal class BatchAccumulator<K : Any, V : Any>(
    private val scope: CoroutineScope,
    private val window: Duration,
    private val maxBatchSize: Int,
    private val fetch: suspend (keys: Set<K>) -> Map<K, V>,
) {
    private val mutex = Mutex()
    private val pending = LinkedHashMap<K, CompletableDeferred<V>>()
    private var flushScheduled = false

    /** Enqueues [key] into the current window and suspends until its batch resolves it. */
    suspend fun load(key: K): V {
        val slot = mutex.withLock {
            val existing = pending[key]
            if (existing != null) {
                existing // a load for this key is already in the window: share its slot
            } else {
                val fresh = CompletableDeferred<V>()
                pending[key] = fresh
                when {
                    pending.size >= maxBatchSize -> flush() // full: dispatch immediately
                    !flushScheduled -> {
                        flushScheduled = true
                        scope.launch {
                            delay(window)
                            mutex.withLock { if (flushScheduled) flush() }
                        }
                    }
                }
                fresh
            }
        }
        return slot.await()
    }

    /** Drains and dispatches the pending batch. Caller holds [mutex]; never suspends. */
    private fun flush() {
        flushScheduled = false
        if (pending.isEmpty()) return
        val batch = LinkedHashMap(pending)
        pending.clear()
        scope.launch { dispatch(batch) }
    }

    private suspend fun dispatch(batch: Map<K, CompletableDeferred<V>>) {
        try {
            val result = fetch(batch.keys)
            for ((key, slot) in batch) {
                val value = result[key]
                if (value != null) slot.complete(value) else slot.completeExceptionally(BatchKeyMissingException(key))
            }
        } catch (cancellation: CancellationException) {
            batch.values.forEach { it.cancel(cancellation) }
            throw cancellation
        } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
            batch.values.forEach { it.completeExceptionally(failure) }
        }
    }
}
