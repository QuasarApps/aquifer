package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.AquiferEvents
import io.github.quasarapps.aquifer.AquiferException
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.SourceOfTruth
import io.github.quasarapps.aquifer.WallClock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

internal class RealAquifer<K : Any, V : Any>(
    private val fetcher: suspend (key: K) -> V,
    private val timeToLive: Duration,
    maxEntries: Int,
    private val clock: WallClock,
    parentScope: CoroutineScope?,
    private val persistence: SourceOfTruth<K, V>? = null,
    private val retry: RetryPolicy = RetryPolicy.NONE,
    private val listener: AquiferEvents<K>? = null,
) : Aquifer<K, V> {

    private val job = SupervisorJob(parentScope?.coroutineContext?.get(Job))
    private val scope = if (parentScope != null) {
        CoroutineScope(parentScope.coroutineContext + job)
    } else {
        CoroutineScope(job + Dispatchers.Default + CoroutineName("Aquifer"))
    }

    private val memory = MemoryCache<K, V>(maxEntries)
    private val inFlight = ConcurrentHashMap<K, Deferred<V>>()
    private val closed = AtomicBoolean(false)

    /** Reference counts of keys with active, fetch-capable stream collectors. */
    private val activeKeys = ConcurrentHashMap<K, Int>()

    /**
     * Write-epoch fencing: every mutation ([put], [invalidate], [invalidateAll]) advances the
     * key's epoch under [commitGuard] and evicts the key's in-flight fetch from the registry.
     * A completing fetch only commits its result (memory, broadcast, persistence) if the
     * epoch it started under is still current — so a response that was already in flight when
     * the data was invalidated or locally edited can never resurrect into the caches, and
     * post-mutation refetches start genuinely new requests instead of joining the doomed one.
     *
     * All epoch bumps, memory mutations, fetch commits, and their persistence writes are
     * serialized by [commitGuard]; cache *reads* never take it.
     */
    private val commitGuard = Mutex()
    private val globalEpoch = AtomicLong()
    private val keyEpochs = ConcurrentHashMap<K, Long>()

    /**
     * Stamps every cache commit with a store-global order, assigned under [commitGuard].
     * Stream collectors use it to drop replayed-out-of-order events; unlike wall-clock
     * timestamps it is immune to same-millisecond ties and backwards clock steps.
     */
    private val sequencer = AtomicLong()

    init {
        // A store whose scope dies (parent cancellation or close()) must report itself
        // closed: operations fail fast instead of hanging on a dead bus.
        job.invokeOnCompletion { closed.set(true) }
    }

    /**
     * Single broadcast bus for all keys; streams filter for their key.
     *
     * Each stream collector drains the bus into an unbounded per-collector buffer running on
     * the store's dispatcher (see [stream]), so engine emissions never block on a slow or
     * stalled downstream collector: one laggy screen can't stall fetch completion, writes, or
     * other keys' streams. The cost is that a stalled collector buffers its own backlog.
     */
    private val events = MutableSharedFlow<Event<K, V>>(extraBufferCapacity = EVENT_BUFFER_CAPACITY)

    /** Context for bus draining: the store's context minus its job, as required by [flowOn]. */
    private val busDrainContext = scope.coroutineContext.minusKey(Job)

    override fun stream(key: K, freshness: Freshness): Flow<DataState<V>> {
        checkOpen()
        return flow {
            checkOpen()
            // CacheOnly collectors never fetch on their own behalf, so they don't make a key
            // "active" for revalidation purposes either.
            val countsAsActive = freshness != Freshness.CacheOnly
            if (countsAsActive) registerActive(key)
            try {
                collectStates(key, freshness)
            } finally {
                if (countsAsActive) unregisterActive(key)
            }
        }.distinctUntilChanged()
    }

    private suspend fun FlowCollector<DataState<V>>.collectStates(key: K, freshness: Freshness) {
        // Captured before hydration so prime() can tell pure LRU eviction (epoch untouched)
        // apart from invalidation when memory comes up empty.
        val preloadEpoch = epochOf(key)
        // Hydrate from persistence BEFORE subscribing to the bus: storage I/O must never run
        // inside the subscription, where a subscriber that is not consuming yet would
        // backpressure every emitter in the store. prime() re-reads memory once subscribed,
        // so anything newer that lands in between is not lost.
        val preloaded = if (freshness == Freshness.NetworkOnly) null else load(key)
        // Last value this collector has seen; backs the `value` field of Loading/Failure.
        var last: V? = null
        // Newest commit sequence this collector has emitted. Bus events buffered before the
        // snapshot was taken replay after it and must not regress us to an earlier commit;
        // the sequence is immune to clock ties and backwards wall-clock steps.
        var newestSequence = Long.MIN_VALUE
        events
            .onSubscription { prime(key, freshness, preloaded, preloadEpoch) }
            .buffer(Channel.UNLIMITED)
            .flowOn(busDrainContext)
            .collect { event ->
                when (event) {
                    is Event.Updated -> if (event.key == key && event.sequence >= newestSequence) {
                        newestSequence = event.sequence
                        last = event.value
                        emit(DataState.Content(event.value, event.origin, isExpired(event.writtenAtMillis)))
                    }

                    is Event.Fetching -> if (event.key == key) {
                        emit(DataState.Loading(last))
                    }

                    is Event.Failed -> if (event.key == key) {
                        emit(DataState.Failure(event.error, last))
                    }

                    is Event.Invalidated -> if (event.key == key) {
                        last = null
                        newestSequence = Long.MIN_VALUE
                        if (freshness != Freshness.CacheOnly) refresh(key)
                    }

                    is Event.ClearedAll -> {
                        last = null
                        newestSequence = Long.MIN_VALUE
                        if (freshness != Freshness.CacheOnly) refresh(key)
                    }
                }
            }
    }

    override suspend fun get(key: K, freshness: Freshness): V {
        checkOpen()
        // NetworkOnly bypasses cached reads entirely: no memory lookup, no persistence I/O.
        val entry = if (freshness == Freshness.NetworkOnly) null else load(key)?.entry
        val usable = entry != null && !isExpired(entry.writtenAtMillis)
        return when (freshness) {
            Freshness.CacheOnly -> entry?.value ?: throw CacheMissException(key)

            Freshness.CacheFirst ->
                if (usable) entry.value else awaitFetch(key, fallback = entry?.value)

            Freshness.StaleWhileRevalidate ->
                if (entry != null) {
                    if (!usable) refresh(key)
                    entry.value
                } else {
                    awaitFetch(key, fallback = null)
                }

            Freshness.NetworkFirst -> awaitFetch(key, fallback = entry?.value)

            Freshness.NetworkOnly -> awaitFetch(key, fallback = null)
        }
    }

    override suspend fun fresh(key: K): V = get(key, Freshness.NetworkOnly)

    override suspend fun put(key: K, value: V) {
        checkOpen()
        val now = clock.nowMillis()
        val entry: MemoryCache.Entry<V>
        commitGuard.withLock {
            // Persist first so a storage failure propagates before anything becomes visible;
            // direct mutations are all-or-nothing, unlike best-effort fetch write-through.
            persistence?.write(key, PersistedEntry(value, now))
            fence(key)
            entry = MemoryCache.Entry(value, now, sequencer.incrementAndGet())
            memory.put(key, entry)
        }
        events.emit(Event.Updated(key, value, Origin.LOCAL, now, entry.sequence))
    }

    override suspend fun invalidate(key: K) {
        checkOpen()
        commitGuard.withLock {
            persistence?.delete(key)
            fence(key)
            memory.remove(key)
        }
        events.emit(Event.Invalidated(key))
    }

    override suspend fun invalidateAll() {
        checkOpen()
        commitGuard.withLock {
            persistence?.deleteAll()
            globalEpoch.incrementAndGet()
            keyEpochs.clear()
            inFlight.clear()
            memory.clear()
        }
        events.emit(Event.ClearedAll)
    }

    /**
     * Invalidates everything an in-flight fetch for [key] might commit: bumps the key's
     * epoch and evicts the fetch from the registry so later refreshes start anew. The fetch
     * itself keeps running — its awaiting callers still get a value — but its commit is
     * discarded. Must run under [commitGuard].
     */
    private fun fence(key: K) {
        keyEpochs[key] = (keyEpochs[key] ?: 0L) + 1L
        inFlight.remove(key)
    }

    /** Epoch snapshot for [key]; commits compare snapshots taken when their fetch started. */
    private fun epochOf(key: K): Pair<Long, Long> = globalEpoch.get() to (keyEpochs[key] ?: 0L)

    override suspend fun revalidateActive() {
        checkOpen()
        for (key in activeKeys.keys) {
            val entry = load(key)?.entry
            if (entry == null || isExpired(entry.writtenAtMillis)) refresh(key)
        }
    }

    override fun revalidateOn(trigger: Flow<*>) {
        checkOpen()
        scope.launch {
            try {
                trigger.collect {
                    try {
                        revalidateActive()
                    } catch (cancellation: CancellationException) {
                        throw cancellation
                    } catch (failure: Throwable) {
                        // One sweep failed (e.g. a throwing storage read). Report it, but
                        // keep the subscription alive — a single bad sweep must not silently
                        // end reconnect revalidation for the rest of the process lifetime.
                        notify { onRevalidationTriggerFailed(failure) }
                    }
                }
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (failure: Throwable) {
                // The trigger flow itself failed. An uncaught exception here would escape
                // the supervisor and crash the host process; instead only this subscription
                // ends and the store keeps working.
                notify { onRevalidationTriggerFailed(failure) }
            }
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            job.cancel()
        }
    }

    // CAS loops on ConcurrentMap primitives instead of merge/computeIfPresent: those default
    // methods only exist on Android API 24+, and aquifer-core supports API 21.
    private fun registerActive(key: K) {
        while (true) {
            val current = activeKeys[key]
            when {
                current == null -> if (activeKeys.putIfAbsent(key, 1) == null) return
                else -> if (activeKeys.replace(key, current, current + 1)) return
            }
        }
    }

    private fun unregisterActive(key: K) {
        while (true) {
            val current = activeKeys[key] ?: return
            when {
                current <= 1 -> if (activeKeys.remove(key, current)) return
                else -> if (activeKeys.replace(key, current, current - 1)) return
            }
        }
    }

    /**
     * Emits the initial snapshot for a new stream collector and triggers a fetch when the
     * requested freshness calls for one. Runs inside [onSubscription] — after the collector
     * is registered with the bus, so no update can slip between the snapshot read and live
     * event delivery. Must stay I/O-free: it executes while this subscriber is not yet
     * consuming, so anything slow here backpressures every emitter in the store.
     */
    private suspend fun FlowCollector<Event<K, V>>.prime(
        key: K,
        freshness: Freshness,
        preloaded: Snapshot<V>?,
        preloadEpoch: Pair<Long, Long>,
    ) {
        // Memory only — the persistence read already happened before subscription (see
        // collectStates); doing I/O here would stall the entire bus. NetworkOnly bypasses
        // cached reads entirely.
        val inMemory = if (freshness == Freshness.NetworkOnly) null else memory.get(key)
        val snapshot = reconcileSnapshot(preloaded, inMemory, epochUnchanged = epochOf(key) == preloadEpoch)
        val entry = snapshot?.entry
        if (snapshot != null) {
            emit(
                Event.Updated(
                    key,
                    snapshot.entry.value,
                    snapshot.origin,
                    snapshot.entry.writtenAtMillis,
                    snapshot.entry.sequence,
                ),
            )
        }
        // A fetch that started before this collector subscribed broadcast its Fetching event
        // too early for us to see it. Note it now (before refresh, so a fetch we start
        // ourselves is not mistaken for a pre-existing one) and replay it below.
        val joinedInFlightFetch = inFlight.containsKey(key)
        val needsValue = entry == null || isExpired(entry.writtenAtMillis)
        val shouldFetch = when (freshness) {
            Freshness.CacheOnly -> false
            Freshness.CacheFirst, Freshness.StaleWhileRevalidate -> needsValue
            Freshness.NetworkFirst, Freshness.NetworkOnly -> true
        }
        if (shouldFetch) refresh(key)
        when {
            joinedInFlightFetch -> emit(Event.Fetching(key))
            !shouldFetch && entry == null -> emit(Event.Failed(key, CacheMissException(key)))
        }
    }

    /**
     * Returns the in-flight fetch for [key], starting one when none is running. The fetch runs
     * in the store's scope so it completes — and lands in the cache — even if every caller that
     * was awaiting it has been cancelled. Progress and outcome are broadcast on the bus.
     */
    private fun refresh(key: K): Deferred<V> {
        inFlight[key]?.let { return it }
        val pending = scope.async(start = CoroutineStart.LAZY) {
            var attempts = 1
            // Any mutation after this point outranks this fetch's result.
            val epoch = epochOf(key)
            try {
                events.emit(Event.Fetching(key))
                notify { onFetchStarted(key) }
                val startedAt = clock.nowMillis()
                val value = fetchWithRetry(key) { attempts = it }
                val now = clock.nowMillis()
                notify { onFetchSucceeded(key, (now - startedAt).milliseconds) }
                val committed: MemoryCache.Entry<V>? = commitGuard.withLock {
                    if (epochOf(key) == epoch) {
                        val entry = MemoryCache.Entry(value, now, sequencer.incrementAndGet())
                        memory.put(key, entry)
                        persistFetched(key, value, now)
                        entry
                    } else {
                        null
                    }
                }
                // Re-check before broadcasting: a mutation may have raced the gap above, and
                // observers should not see a fenced-off value even transiently.
                if (committed != null && epochOf(key) == epoch) {
                    events.emit(Event.Updated(key, value, Origin.FETCHER, now, committed.sequence))
                }
                value
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (failure: Throwable) {
                notify { onFetchFailed(key, failure, attempts) }
                // A failure of a fenced-off fetch is stale news; observers already moved on.
                if (epochOf(key) == epoch) {
                    events.emit(Event.Failed(key, failure))
                }
                throw failure
            }
        }
        val existing = inFlight.putIfAbsent(key, pending)
        if (existing != null) {
            // Lost the race: another caller registered a fetch first. Join theirs.
            pending.cancel()
            return existing
        }
        pending.invokeOnCompletion { inFlight.remove(key, pending) }
        pending.start()
        return pending
    }

    /**
     * Loads the freshest locally available entry: memory first, then the source of truth.
     * A persistence hit hydrates the memory cache — preserving the original write timestamp,
     * so staleness decisions remain correct across process restarts and LRU evictions.
     *
     * Hydration is epoch-fenced exactly like fetch commits: a storage read suspended across
     * an [invalidate]/[invalidateAll] must not put the deleted entry back into memory; the
     * fenced case reports a miss. Memory is also re-checked under the lock — a commit that
     * raced the storage read (a fetch that just returned, or a put, which also moves the
     * epoch) is fresher than the disk snapshot by construction and must never be overwritten
     * by it. The hot memory path never takes [commitGuard].
     */
    private suspend fun load(key: K): Snapshot<V>? {
        memory.get(key)?.let { return Snapshot(it, Origin.MEMORY) }
        val store = persistence ?: return null
        val epoch = epochOf(key)
        val persisted = store.read(key) ?: return null
        return commitGuard.withLock {
            val existing = memory.get(key)
            when {
                existing != null -> Snapshot(existing, Origin.MEMORY)

                epochOf(key) == epoch -> {
                    val entry =
                        MemoryCache.Entry(persisted.value, persisted.writtenAtMillis, sequencer.incrementAndGet())
                    memory.put(key, entry)
                    Snapshot(entry, Origin.PERSISTENCE)
                }

                else -> null
            }
        }
    }

    /**
     * Best-effort write-through after a successful fetch: a storage failure must not turn a
     * successful fetch into an error (the value is already cached and broadcast), so anything
     * but cancellation is swallowed. Direct mutations ([put], [invalidate]) propagate instead.
     */
    private suspend fun persistFetched(key: K, value: V, writtenAtMillis: Long) {
        val store = persistence ?: return
        try {
            store.write(key, PersistedEntry(value, writtenAtMillis))
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (failure: Throwable) {
            // Swallowed by design (the fetch already succeeded); surfaced to observers only.
            notify { onPersistenceWriteFailed(key, failure) }
        }
    }

    internal data class Snapshot<V : Any>(
        val entry: MemoryCache.Entry<V>,
        val origin: Origin,
    )

    /**
     * Runs the fetcher with the configured [retry] policy. Reports the current attempt number
     * through [onAttempt] so the caller can attribute a terminal failure correctly, and
     * notifies the listener before each backoff sleep. Cancellation is never retried.
     */
    private suspend fun fetchWithRetry(key: K, onAttempt: (Int) -> Unit): V {
        var attempt = 1
        while (true) {
            try {
                return fetcher(key)
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (failure: Throwable) {
                val nextDelay = retry.delayAfter(attempt, failure) ?: throw failure
                notify { onFetchRetried(key, attempt, failure, nextDelay) }
                attempt++
                onAttempt(attempt)
                delay(nextDelay)
            }
        }
    }

    /** Listener callbacks must never disturb the engine: anything they throw is swallowed. */
    private inline fun notify(block: AquiferEvents<K>.() -> Unit) {
        val observer = listener ?: return
        try {
            observer.block()
        } catch (_: Throwable) {
            // Misbehaving listeners are intentionally ignored.
        }
    }

    /** Awaits the shared fetch, falling back to [fallback] when the fetch fails (stale-if-error). */
    private suspend fun awaitFetch(key: K, fallback: V?): V {
        try {
            return refresh(key).await()
        } catch (cancellation: CancellationException) {
            // If the *caller* was cancelled, propagate that as normal cancellation. Otherwise
            // it was the fetch that died — the store closed underneath us — and silently
            // rethrowing the CancellationException would cancel an active caller without any
            // visible error.
            coroutineContext.ensureActive()
            throw AquiferException("Aquifer was closed while fetching '$key'", cancellation)
        } catch (failure: Throwable) {
            return fallback ?: throw failure
        }
    }

    private fun isExpired(writtenAtMillis: Long): Boolean {
        if (timeToLive == Duration.INFINITE) return false
        return clock.nowMillis() - writtenAtMillis >= timeToLive.inWholeMilliseconds
    }

    private fun checkOpen() = check(!closed.get()) { "Aquifer is closed" }

    internal companion object {
        const val EVENT_BUFFER_CAPACITY = 64

        /**
         * Resolves a new collector's initial snapshot from the pre-subscription hydration
         * ([preloaded]) and the entry currently in memory:
         *
         * - Memory wins when present; if it is the very commit we hydrated (same sequence),
         *   the preloaded snapshot is kept so its true origin (PERSISTENCE) is reported.
         * - When memory is empty, an unchanged epoch proves the gap was pure LRU eviction
         *   and the hydrated snapshot is still valid; a moved epoch means the key was
         *   invalidated after hydration, so the snapshot must be dropped.
         */
        fun <V : Any> reconcileSnapshot(
            preloaded: Snapshot<V>?,
            inMemory: MemoryCache.Entry<V>?,
            epochUnchanged: Boolean,
        ): Snapshot<V>? = when {
            inMemory != null ->
                if (preloaded != null && preloaded.entry.sequence == inMemory.sequence) {
                    preloaded
                } else {
                    Snapshot(inMemory, Origin.MEMORY)
                }

            preloaded != null && epochUnchanged -> preloaded

            else -> null
        }
    }
}
