package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.AquiferEvents
import io.github.quasarapps.aquifer.AquiferException
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.FetchResult
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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// The engine is deliberately one cohesive class — single-flight, fencing, and the update bus
// are one interlocking mechanism — and its internal constructor is fed by the builder, so
// splitting either to satisfy the thresholds would hurt navigability without adding safety.
@Suppress("TooManyFunctions", "LongParameterList")
internal class RealAquifer<K : Any, V : Any>(
    private val fetcher: suspend (key: K, validator: String?) -> FetchResult<V>,
    /** Whether [fetcher] consults validators; plain stores skip the pre-fetch entry read. */
    private val conditional: Boolean,
    /** Failure-memory parameters; `null` disables negative caching entirely. */
    private val negativeCache: NegativeCachePolicy? = null,
    private val timeToLive: Duration,
    /** Fraction in `[0, 1]` deterministically shortening each entry's effective TTL; 0 = off. */
    private val ttlJitter: Double = 0.0,
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
     * Stamps every cache commit — and every drop ([invalidate]/[invalidateAll]) — with a
     * store-global order, assigned under [commitGuard]. Stream collectors use it to reject
     * replayed- or delivered-out-of-order events; unlike wall-clock timestamps it is immune
     * to same-millisecond ties and backwards clock steps.
     */
    private val sequencer = AtomicLong()

    /**
     * Failure memory for negative caching (used only when [negativeCache] is set): the last
     * terminal failure per key with its suppression deadline. Entries are removed only on a
     * successful commit or on [put]/[invalidate]/[invalidateAll] — an *expired* entry stays,
     * no longer suppressing but still carrying the consecutive-failure streak, so a
     * chronically failing key keeps its stretched window between spaced-out probes.
     */
    private val negative = ConcurrentHashMap<K, NegativeEntry>()

    private class NegativeEntry(
        val error: Throwable,
        val consecutiveFailures: Int,
        val suppressUntilMillis: Long,
    )

    /**
     * The failure currently suppressing strategy-driven fetches of [key], or `null` when
     * fetching is allowed. An expired window stops suppressing but its record is kept — the
     * consecutive-failure streak resets only on success or mutation. Each suppressed read
     * is reported via [AquiferEvents.onFetchSuppressed].
     */
    private fun suppression(key: K): NegativeEntry? {
        if (negativeCache == null) return null
        val entry = negative[key] ?: return null
        val now = clock.nowMillis()
        // Expired: fetching is allowed again, but the record stays — the streak resets only
        // on success or mutation, never by the mere passage of time.
        if (now >= entry.suppressUntilMillis) return null
        notify { onFetchSuppressed(key, entry.error, (entry.suppressUntilMillis - now).milliseconds) }
        return entry
    }

    /** Serves [fallback] (stale-if-error without re-fetching) or rethrows the remembered
     *  failure while [key] is suppressed; otherwise fetches via [fetch]. */
    private suspend fun fetchUnlessSuppressed(key: K, fallback: V?, fetch: suspend () -> V): V {
        val block = suppression(key) ?: return fetch()
        return fallback ?: throw block.error
    }

    /** Remembers a terminal [failure] of [key]; consecutive failures stretch the window. */
    private fun recordFailure(key: K, failure: Throwable) {
        val policy = negativeCache ?: return
        val failures = (negative[key]?.consecutiveFailures ?: 0) + 1
        val window = policy.windowFor(failures)
        // Millisecond deadlines on a millisecond clock: round the window UP, so a
        // sub-millisecond timeToLive still suppresses until the next tick instead of
        // truncating to "never" (the same hazard isExpired avoids for maxAge).
        val floor = window.inWholeMilliseconds
        val windowMillis = if (floor.milliseconds < window) floor + 1 else floor
        negative[key] = NegativeEntry(failure, failures, clock.nowMillis() + windowMillis)
    }

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

    override fun stream(key: K, freshness: Freshness, maxAge: Duration?): Flow<DataState<V>> {
        checkOpen()
        requireValidMaxAge(maxAge)
        return flow {
            checkOpen()
            // CacheOnly collectors never fetch on their own behalf, so they don't make a key
            // "active" for revalidation purposes either.
            val countsAsActive = freshness != Freshness.CacheOnly
            if (countsAsActive) registerActive(key)
            try {
                collectStates(key, freshness, maxAge)
            } finally {
                if (countsAsActive) unregisterActive(key)
            }
        }.distinctUntilChanged()
    }

    private suspend fun FlowCollector<DataState<V>>.collectStates(key: K, freshness: Freshness, maxAge: Duration?) {
        // Captured before hydration so prime() can tell pure LRU eviction (epoch untouched)
        // apart from invalidation when memory comes up empty.
        val preloadEpoch = epochOf(key)
        // Hydrate from persistence BEFORE subscribing to the bus: storage I/O must never run
        // inside the subscription, where a subscriber that is not consuming yet would
        // backpressure every emitter in the store. prime() re-reads memory once subscribed,
        // so anything newer that lands in between is not lost.
        val preloaded = if (freshness == Freshness.NetworkOnly) null else load(key)
        val tracker = StreamTracker(key, freshness, maxAge, downstream = this)
        events
            .onSubscription { prime(key, freshness, maxAge, preloaded, preloadEpoch) }
            .buffer(Channel.UNLIMITED)
            .flowOn(busDrainContext)
            .collect { event -> tracker.onEvent(event) }
    }

    /**
     * One stream collector's view of the bus: translates engine events into [DataState]s for
     * [downstream], tracking the last value seen and the newest commit sequence emitted.
     */
    private inner class StreamTracker(
        private val key: K,
        private val freshness: Freshness,
        private val maxAge: Duration?,
        private val downstream: FlowCollector<DataState<V>>,
    ) {
        /** Last value this collector has seen; backs the `value` field of Loading/Failure. */
        private var last: V? = null

        /**
         * Newest commit sequence this collector has applied. Bus events buffered before the
         * prime snapshot replay after it, and a commit's bus emission happens outside
         * [commitGuard] so two racing mutations can reach the bus in the opposite of their
         * commit order — either way, an event older than the watermark must be rejected,
         * never applied. The sequence is immune to clock ties and backwards clock steps.
         */
        private var newestSequence = Long.MIN_VALUE

        suspend fun onEvent(event: Event<K, V>) {
            when (event) {
                is Event.Updated -> if (event.key == key && event.sequence >= newestSequence) {
                    newestSequence = event.sequence
                    last = event.value
                    downstream.emit(
                        DataState.Content(event.value, event.origin, isExpired(event.writtenAtMillis, maxAge)),
                    )
                }

                is Event.Fetching -> if (event.key == key) {
                    downstream.emit(DataState.Loading(last))
                }

                is Event.Failed -> if (event.key == key) {
                    downstream.emit(DataState.Failure(event.error, last))
                }

                is Event.Absent -> if (event.key == key) {
                    downstream.emit(DataState.Empty)
                }

                is Event.Invalidated -> if (event.key == key) {
                    onDrop(event.sequence)
                }

                is Event.ClearedAll -> onDrop(event.sequence)
            }
        }

        /**
         * The observed key (or the whole store) was dropped at [sequence] in the commit
         * order. The watermark guard mirrors [Event.Updated]'s: a drop that lost the emit
         * race to a newer write must be rejected, or this collector would erase that write —
         * and conversely, once a drop is applied, a stale buffered write can no longer
         * resurrect deleted data (which a CacheOnly stream, having no refetch, would
         * otherwise render forever).
         *
         * Fetch-capable strategies react by refetching — their `Loading(null)` communicates
         * the loss; cache-only ones get [DataState.Empty] instead, since they cannot fetch.
         */
        private suspend fun onDrop(sequence: Long) {
            if (sequence < newestSequence) return
            newestSequence = sequence
            last = null
            if (freshness != Freshness.CacheOnly) refresh(key) else downstream.emit(DataState.Empty)
        }
    }

    override suspend fun get(key: K, freshness: Freshness, maxAge: Duration?): V {
        checkOpen()
        requireValidMaxAge(maxAge)
        // NetworkOnly bypasses cached reads entirely: no memory lookup, no persistence I/O.
        val entry = if (freshness == Freshness.NetworkOnly) null else load(key)?.entry
        val usable = entry != null && !isExpired(entry.writtenAtMillis, maxAge)
        return when (freshness) {
            Freshness.CacheOnly -> entry?.value ?: throw CacheMissException(key)

            Freshness.CacheFirst ->
                if (usable) {
                    entry.value
                } else {
                    fetchUnlessSuppressed(key, fallback = entry?.value) {
                        awaitFetch(key, fallback = entry?.value)
                    }
                }

            Freshness.StaleWhileRevalidate ->
                if (entry != null) {
                    if (!usable && suppression(key) == null) refresh(key)
                    entry.value
                } else {
                    fetchUnlessSuppressed(key, fallback = null) { awaitFetch(key, fallback = null) }
                }

            Freshness.NetworkFirst ->
                fetchUnlessSuppressed(key, fallback = entry?.value) {
                    awaitFetch(key, fallback = entry?.value)
                }

            // Deliberately bypasses the negative cache: an explicit demand for the network.
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
            negative.remove(key)
            entry = MemoryCache.Entry(value, now, sequencer.incrementAndGet())
            memory.put(key, entry)
        }
        events.emit(Event.Updated(key, value, Origin.LOCAL, now, entry.sequence))
    }

    override suspend fun invalidate(key: K) {
        checkOpen()
        // The drop takes a slot in the same commit order as writes: bus emission happens
        // outside the lock, so collectors need the sequence to arbitrate emit races.
        val sequence = commitGuard.withLock {
            persistence?.delete(key)
            fence(key)
            negative.remove(key)
            memory.remove(key)
            sequencer.incrementAndGet()
        }
        events.emit(Event.Invalidated(key, sequence))
    }

    override suspend fun invalidateAll() {
        checkOpen()
        val sequence = commitGuard.withLock {
            persistence?.deleteAll()
            globalEpoch.incrementAndGet()
            keyEpochs.clear()
            inFlight.clear()
            negative.clear()
            memory.clear()
            sequencer.incrementAndGet()
        }
        events.emit(Event.ClearedAll(sequence))
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
            if ((entry == null || isExpired(entry.writtenAtMillis)) && suppression(key) == null) refresh(key)
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
                    } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
                        // One sweep failed (e.g. a throwing storage read). Report it, but
                        // keep the subscription alive — a single bad sweep must not silently
                        // end reconnect revalidation for the rest of the process lifetime.
                        notify { onRevalidationTriggerFailed(failure) }
                    }
                }
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
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
        maxAge: Duration?,
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
        val needsValue = entry == null || isExpired(entry.writtenAtMillis, maxAge)
        val wantsFetch = wantsFetch(freshness, needsValue)
        // NetworkOnly bypasses the negative cache (explicit network demand); the other
        // fetch-capable strategies stand down while a recent failure is remembered.
        val block = if (wantsFetch && freshness != Freshness.NetworkOnly) suppression(key) else null
        if (wantsFetch && block == null) refresh(key)
        when {
            joinedInFlightFetch -> emit(Event.Fetching(key))
            // A suppressed, valueless subscriber still deserves a state: the remembered
            // failure, exactly as if it had just happened.
            block != null && entry == null -> emit(Event.Failed(key, block.error))
            // Only CacheOnly reaches here valueless (the staleness-aware strategies fetch on
            // a miss): an affirmative "nothing cached, nothing will fetch", not a failure.
            !wantsFetch && entry == null -> emit(Event.Absent(key))
        }
    }

    /** Whether [freshness] calls for a fetch given the snapshot, before negative-cache gating. */
    private fun wantsFetch(freshness: Freshness, needsValue: Boolean): Boolean = when (freshness) {
        Freshness.CacheOnly -> false
        Freshness.CacheFirst, Freshness.StaleWhileRevalidate -> needsValue
        Freshness.NetworkFirst, Freshness.NetworkOnly -> true
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
                // The validator — and the value a NotModified resolves to — come from the
                // entry as it stood when the fetch started; a mutation during the fetch
                // fences the commit regardless. Plain stores skip this read entirely.
                val prior = if (conditional) load(key)?.entry else null
                val startedAt = clock.nowMillis()
                val result = fetchWithRetry(key, prior?.validator) { attempts = it }
                val now = clock.nowMillis()
                notify { onFetchSucceeded(key, (now - startedAt).milliseconds) }
                val (value, validator) = resolve(key, result, prior)
                val committed: MemoryCache.Entry<V>? = commitGuard.withLock {
                    if (epochOf(key) == epoch) {
                        // Cleared with the commit it celebrates: a read between commit and a
                        // later clear could otherwise still see the stale suppression window.
                        negative.remove(key)
                        val entry = MemoryCache.Entry(value, now, sequencer.incrementAndGet(), validator)
                        memory.put(key, entry)
                        persistFetched(key, value, now, validator)
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
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
                notify { onFetchFailed(key, failure, attempts) }
                // A failure of a fenced-off fetch is stale news; observers already moved on.
                // The check and the record commit together under the guard, so a racing
                // put/invalidate can't have its just-cleared failure memory re-poisoned by
                // a terminal failure that observed the pre-mutation epoch.
                val current = commitGuard.withLock {
                    (epochOf(key) == epoch).also { if (it) recordFailure(key, failure) }
                }
                if (current) {
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
                        MemoryCache.Entry(
                            persisted.value,
                            persisted.writtenAtMillis,
                            sequencer.incrementAndGet(),
                            persisted.validator,
                        )
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
    private suspend fun persistFetched(key: K, value: V, writtenAtMillis: Long, validator: String?) {
        val store = persistence ?: return
        try {
            store.write(key, PersistedEntry(value, writtenAtMillis, validator))
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
            // Swallowed by design (the fetch already succeeded); surfaced to observers only.
            notify { onPersistenceWriteFailed(key, failure) }
        }
    }

    internal data class Snapshot<V : Any>(
        val entry: MemoryCache.Entry<V>,
        val origin: Origin,
    )

    /**
     * Resolves what a fetch [result] commits: a [FetchResult.Fresh] carries its own value
     * and validator; a [FetchResult.NotModified] re-commits the [prior] entry — "still
     * current" only means something against a cached entry, whose age the commit resets.
     */
    private fun resolve(key: K, result: FetchResult<V>, prior: MemoryCache.Entry<V>?): Pair<V, String?> =
        when (result) {
            is FetchResult.Fresh -> result.value to result.validator
            is FetchResult.NotModified -> {
                val entry = prior
                // A prior entry alone is not enough: one without a validator (a local put,
                // or persisted before validators existed) never sent a revalidation token,
                // so there is nothing a NotModified could be "not modified" against.
                check(entry != null && entry.validator != null) {
                    "Fetcher returned NotModified for '$key' but was given no validator " +
                        "(nothing cached, or the cached entry carries none)"
                }
                entry.value to entry.validator
            }
        }

    /**
     * Runs the fetcher with the configured [retry] policy. Reports the current attempt number
     * through [onAttempt] so the caller can attribute a terminal failure correctly, and
     * notifies the listener before each backoff sleep. Cancellation is never retried.
     */
    private suspend fun fetchWithRetry(key: K, validator: String?, onAttempt: (Int) -> Unit): FetchResult<V> {
        var attempt = 1
        while (true) {
            try {
                return fetcher(key, validator)
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
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
        } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
            return fallback ?: throw failure
        }
    }

    /**
     * Staleness against [maxAge] when given (per-call override, never jittered — it is the
     * caller's explicit bar), else the store-wide TTL with the entry's deterministic jitter.
     */
    private fun isExpired(writtenAtMillis: Long, maxAge: Duration? = null): Boolean {
        val horizon = maxAge ?: jitteredTimeToLive(writtenAtMillis)
        // Duration arithmetic, not inWholeMilliseconds: a sub-millisecond horizon must not
        // truncate to zero and declare everything instantly stale. INFINITE compares false
        // against any finite elapsed time, giving "always fresh" naturally.
        return (clock.nowMillis() - writtenAtMillis).milliseconds >= horizon
    }

    /** This entry's effective TTL: scaled by a stable factor of its own write timestamp. */
    private fun jitteredTimeToLive(writtenAtMillis: Long): Duration {
        if (ttlJitter == 0.0 || timeToLive == Duration.INFINITE) return timeToLive
        return timeToLive * (1.0 - ttlJitter * TtlJitter.fractionFor(writtenAtMillis))
    }

    private fun requireValidMaxAge(maxAge: Duration?) {
        // INFINITE is allowed and meaningful ("serve anything cached"), unlike retry delays.
        require(maxAge == null || maxAge.isPositive()) {
            "maxAge must be positive, was $maxAge"
        }
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
