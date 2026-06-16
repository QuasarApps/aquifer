package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.BatchKeyMissingException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
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
 * The mutex guards only the in-memory enqueue/flush bookkeeping (an O(batch) drain when a
 * window dispatches) — never the [fetch] call or any `await` — so a slow backend can't stall
 * enqueuers. All scheduling runs in [scope], and a
 * [load] only ever runs inside that scope (the engine calls it from the per-key fetch
 * coroutine), so closing the store cancels every awaiter rather than leaving a slot dangling.
 * Under virtual time the [window] delay is skipped while the test is idle, so a synchronous
 * burst of loads still coalesces.
 */
internal class BatchAccumulator<K : Any, V : Any>(
    private val scope: CoroutineScope,
    private val window: Duration,
    private val maxBatchSize: Int,
    private val fetch: suspend (keys: Set<K>) -> Map<K, V>,
) {
    private val mutex = Mutex()
    private val pending = LinkedHashMap<K, CompletableDeferred<V>>()

    /** The current window's flush timer, or `null` when no window is open. Guarded by [mutex]. */
    private var flushTimer: Job? = null

    /** Enqueues [key] into the current window and suspends until its batch resolves it. */
    suspend fun load(key: K): V {
        // Drained under the lock, but dispatched *after* releasing it: launching the fetch
        // inside `withLock` could, on an immediate dispatcher, run it inline while the lock is
        // still held and stall enqueuers.
        var ready: Map<K, CompletableDeferred<V>>? = null
        val slot = mutex.withLock {
            val existing = pending[key]
            if (existing != null) {
                existing // a load for this key is already in the window: share its slot
            } else {
                val fresh = CompletableDeferred<V>()
                pending[key] = fresh
                when {
                    pending.size >= maxBatchSize -> {
                        // Full: cancel the open window's timer and dispatch immediately.
                        flushTimer?.cancel()
                        flushTimer = null
                        ready = drain()
                    }
                    flushTimer == null -> flushTimer = scope.launch {
                        delay(window)
                        val batch = mutex.withLock {
                            flushTimer = null // this window is firing; let the next load reschedule
                            drain()
                        }
                        if (batch.isNotEmpty()) dispatch(batch)
                    }
                }
                fresh
            }
        }
        ready?.let { scope.launch { dispatch(it) } }
        return slot.await()
    }

    /** Removes and returns the pending batch (empty if none). Caller holds [mutex]. */
    private fun drain(): Map<K, CompletableDeferred<V>> {
        if (pending.isEmpty()) return emptyMap()
        val batch = LinkedHashMap(pending)
        pending.clear()
        return batch
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
