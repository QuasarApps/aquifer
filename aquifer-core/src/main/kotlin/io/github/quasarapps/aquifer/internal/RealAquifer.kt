package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.AquiferEvents
import io.github.quasarapps.aquifer.AquiferException
import io.github.quasarapps.aquifer.BatchKeyMissingException
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.CacheStats
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.FetchResult
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.SourceOfTruth
import io.github.quasarapps.aquifer.WallClock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
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
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// The engine is deliberately one cohesive class — single-flight, fencing, and the update bus
// are one interlocking mechanism — and its internal constructor is fed by the builder, so
// splitting either to satisfy the thresholds would hurt navigability without adding safety.
@Suppress("TooManyFunctions", "LongParameterList", "LargeClass")
internal class RealAquifer<K : Any, V : Any>(
    private val fetcher: suspend (key: K, validator: String?) -> FetchResult<V>,
    /** Whether [fetcher] consults validators; plain stores skip the pre-fetch entry read. */
    private val conditional: Boolean,
    /** Resolves many keys in one backend call; `null` means [getAll] fetches keys individually. */
    private val batchFetcher: (suspend (keys: Set<K>) -> Map<K, V>)? = null,
    /** Validator-aware batch fetcher (ETag/304 batched); mutually exclusive with [batchFetcher]. */
    private val conditionalBatchFetcher: (suspend (validators: Map<K, String?>) -> Map<K, FetchResult<V>>)? = null,
    /** Auto-coalescing window for single-key fetches; [Duration.ZERO] disables it. */
    private val coalesceWindow: Duration = Duration.ZERO,
    /** Dispatch a coalesced batch early once this many distinct keys accumulate. */
    private val maxBatchSize: Int = Int.MAX_VALUE,
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

    // stats() counters. Reads run on many coroutines, so these are lock-free; evictions live on
    // MemoryCache and in-flight is the inFlight gauge, both read at snapshot time.
    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)

    /**
     * DataLoader-style auto-coalescing for single-key fetches, present only when a batch
     * fetcher is configured with a positive window. `getAll` keeps its own immediate batch.
     */
    private val accumulator: BatchAccumulator<K, V>? =
        if (batchFetcher != null && coalesceWindow > Duration.ZERO) {
            BatchAccumulator(scope, coalesceWindow, maxBatchSize, batchFetcher)
        } else {
            null
        }

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
        // One clock read shared with the deadline check: since activeSuppression returns non-null
        // only when now < suppressUntilMillis, the reported remaining is always positive.
        val now = clock.nowMillis()
        val entry = activeSuppression(key, now) ?: return null
        notify { onFetchSuppressed(key, entry.error, (entry.suppressUntilMillis - now).milliseconds) }
        return entry
    }

    /**
     * The live suppression for [key] **without** the [AquiferEvents.onFetchSuppressed] side
     * effect — the pure predicate behind [suppression]. Used where reporting here would
     * double-fire (streamMany's pre-batch gate, which leaves the single report to each key's
     * stream [prime]). [now] defaults to a fresh read; [suppression] passes its own so the
     * reported remaining is computed against the same instant the deadline is checked against.
     */
    private fun activeSuppression(key: K, now: Long = clock.nowMillis()): NegativeEntry? {
        if (negativeCache == null) return null
        val entry = negative[key] ?: return null
        // Expired: fetching is allowed again, but the record stays — the streak resets only
        // on success or mutation, never by the mere passage of time.
        return if (now >= entry.suppressUntilMillis) null else entry
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
                        DataState.Content(
                            event.value,
                            event.origin,
                            isExpired(
                                key,
                                event.writtenAtMillis,
                                maxAge,
                                entryFreshFor = event.serverFreshForMillis?.milliseconds,
                            ),
                        ),
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

    override fun streamMany(keys: Set<K>, freshness: Freshness): Flow<Map<K, DataState<V>>> {
        checkOpen()
        if (keys.isEmpty()) return flowOf(emptyMap())
        // Snapshot the key set once, at call time: the batched pre-fetch and the per-key streams
        // both run later (on collection), so a caller that mutates the Set it passed in between
        // must not desync the two — they must observe the same members.
        val members = LinkedHashSet(keys)
        return flow {
            checkOpen()
            // Batch the initial fetches: register one shared batchFetcher call for the keys that
            // need fetching *before* the per-key streams subscribe, so each stream joins the
            // in-flight fetch instead of firing its own single-key call — the reactive twin of
            // getAll's batching. With no batch fetcher this still single-flight-dedups; the
            // streams just fetch individually. A coalescing window would batch them anyway, but
            // this makes streamMany batch even without one (and dispatches immediately, like
            // getAll), matching getAll's promise.
            try {
                // reportSuppression = false: each member's stream prime reports onFetchSuppressed,
                // so the pre-batch gate must stay silent or a suppressed key would fire it twice.
                startBatch(keysNeedingFetch(members, freshness, reportSuppression = false))
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") ignored: Throwable) {
                // A pre-batch cache read threw: degrade to per-key fetching — each member stream
                // still does its own load and fetch below, and surfaces the error for real.
            }
            combine(members.map { key -> stream(key, freshness) }) { states ->
                buildMap(members.size) { members.forEachIndexed { index, key -> put(key, states[index]) } }
            }.collect { emit(it) }
        }
    }

    override suspend fun get(key: K, freshness: Freshness, maxAge: Duration?): V {
        checkOpen()
        requireValidMaxAge(maxAge)
        // NetworkOnly bypasses cached reads entirely: no memory lookup, no persistence I/O.
        val entry = if (freshness == Freshness.NetworkOnly) null else load(key)?.entry
        val usable = entry != null &&
            !isExpired(key, entry.writtenAtMillis, maxAge, entry.serverFreshForMillis?.milliseconds)
        recordRead(freshness, present = entry != null, usable = usable)
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

    override fun prefetch(key: K, freshness: Freshness) {
        checkOpen()
        if (freshness == Freshness.CacheOnly) return // never fetches; nothing to warm
        // Fire-and-forget in the store's scope: returns immediately, and the warmed value
        // lands in the cache through refresh()'s normal commit. Mirrors get()'s fetch
        // decision (freshness + negative caching) but triggers rather than awaits.
        scope.launch {
            try {
                // Only the staleness-aware strategies need a cache read to decide; the
                // always-fetch ones (NetworkFirst/NetworkOnly) skip the I/O, so a swallowed
                // read error can't turn an always-fetch prefetch into a silent no-op.
                val needsValue = when (freshness) {
                    Freshness.CacheFirst, Freshness.StaleWhileRevalidate ->
                        load(key)?.entry?.let {
                            isExpired(key, it.writtenAtMillis, entryFreshFor = it.serverFreshForMillis?.milliseconds)
                        } ?: true
                    else -> true
                }
                // Same fetch decision as prime/get: suppression is consulted only when a
                // fetch is actually wanted, so a still-fresh entry reports nothing.
                val wantsFetch = wantsFetch(freshness, needsValue)
                if (wantsFetch && fetchBlock(key, wantsFetch, freshness) == null) refresh(key)
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") ignored: Throwable) {
                // A prefetch is best-effort: a failing cache read here must not escape the
                // store scope (an uncaught throw would crash the host). The fetcher's own
                // failures already surface through AquiferEvents inside refresh(), and the
                // next real read re-hits the same read path and reports it for real.
            }
        }
    }

    override fun prefetchAll(keys: Set<K>, freshness: Freshness) {
        checkOpen()
        if (keys.isEmpty() || freshness == Freshness.CacheOnly) return // CacheOnly never fetches
        // Snapshot before launching: the fetch decision runs later in the store scope, so a
        // caller that mutates the Set it passed right after this call must not change the batch.
        val members = LinkedHashSet(keys)
        // Fire-and-forget like prefetch, but collapsed into one backend call like getAll: decide
        // which keys actually want a fetch (freshness + the negative-cache gate), then dispatch
        // them as one batch. The fetches run and commit in the store scope; we never await them,
        // and per-key failures surface through AquiferEvents — never thrown to the caller.
        scope.launch {
            try {
                // reportSuppression = true: prefetchAll has no per-key stream, so it is the one
                // place a suppressed key's onFetchSuppressed is reported (as plain prefetch does).
                startBatch(keysNeedingFetch(members, freshness, reportSuppression = true))
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") ignored: Throwable) {
                // Best-effort: a failing cache read must not escape the scope and crash the host.
            }
        }
    }

    override suspend fun getAll(keys: Set<K>, freshness: Freshness): Map<K, V> {
        checkOpen()
        if (keys.isEmpty()) return emptyMap()
        // Per key: snapshot the cache and decide who needs a network fetch, with get's exact
        // primitives (wantsFetch + the negative-cache gate). One-shot, so a wanted fetch is
        // awaited — there is no SWR background revalidation here (collect stream() per key for
        // serve-stale-then-revalidate).
        val cached = LinkedHashMap<K, V>()
        val toFetch = LinkedHashSet<K>()
        // Reads every non-NetworkOnly key in one batched load — unlike keysNeedingFetch — because
        // it also needs the cached value for stale-if-error fallback, not just the fetch decision.
        val loaded = if (freshness == Freshness.NetworkOnly) emptyMap() else loadAll(keys)
        for (key in keys) {
            val entry = loaded[key]?.entry
            if (entry != null) cached[key] = entry.value
            val usable = entry != null &&
                !isExpired(key, entry.writtenAtMillis, entryFreshFor = entry.serverFreshForMillis?.milliseconds)
            // getAll awaits a stale SWR refresh (get/stream serve stale immediately and revalidate
            // in the background), so for stats a stale-but-present SWR key here is served via an
            // awaited fetch — a miss. Classify SWR like CacheFirst on this path only.
            val readStrategy =
                if (freshness == Freshness.StaleWhileRevalidate) Freshness.CacheFirst else freshness
            recordRead(readStrategy, present = entry != null, usable = usable)
            if (shouldFetch(key, freshness, entry, report = true)) toFetch += key
        }
        val fetched = batchRefresh(toFetch)
        // Prefer the freshly-fetched value, fall back to the cached one (stale-if-error); a
        // key that neither resolved nor had a cached fallback is omitted from the result.
        return buildMap(keys.size) {
            for (key in keys) (fetched[key] ?: cached[key])?.let { put(key, it) }
        }
    }

    /**
     * The single fetch decision for the batch read paths: whether [key] should hit the network
     * under [freshness] given its already-loaded [entry] (or `null` when nothing usable is
     * cached, or the caller skipped the read for an always-fetch strategy) — [wantsFetch]
     * composed with the negative-cache [fetchBlock] gate ([report] forwards to it). [getAll] and
     * [keysNeedingFetch] both route their decision through here so there is one source of truth;
     * they differ only in cache-read policy, which each owns.
     */
    private fun shouldFetch(key: K, freshness: Freshness, entry: MemoryCache.Entry<V>?, report: Boolean): Boolean {
        val needsValue = entry == null ||
            isExpired(key, entry.writtenAtMillis, entryFreshFor = entry.serverFreshForMillis?.milliseconds)
        val wantsFetch = wantsFetch(freshness, needsValue)
        return wantsFetch && fetchBlock(key, wantsFetch, freshness, report) == null
    }

    /**
     * The keys among [keys] that need a network fetch under [freshness], in iteration order.
     * Mirrors [prefetch]'s cache-read policy: only the staleness-aware strategies read the
     * cache; the always-fetch ones ([Freshness.NetworkFirst]/[Freshness.NetworkOnly]) skip the
     * I/O, so a read failure swallowed by [prefetchAll]'s best-effort catch can never turn an
     * always-fetch warmup into a silent no-op (and the wasted I/O is avoided). The decision
     * itself is [shouldFetch]. Shared by the fire-and-forget batch entry points ([prefetchAll],
     * [streamMany]'s pre-trigger); [getAll] keeps its own read because it also captures the
     * cached values for stale-if-error fallback.
     *
     * [reportSuppression] controls whether a suppressed key fires
     * [AquiferEvents.onFetchSuppressed] here: [prefetchAll] reports (it has no per-key stream),
     * while [streamMany] passes `false` and leaves the single report to each key's stream
     * [prime] — so a negative-cached member is announced once, not twice.
     */
    private suspend fun keysNeedingFetch(keys: Set<K>, freshness: Freshness, reportSuppression: Boolean): Set<K> {
        val toFetch = LinkedHashSet<K>()
        val loaded = when (freshness) {
            Freshness.CacheFirst, Freshness.StaleWhileRevalidate -> loadAll(keys)
            else -> emptyMap()
        }
        for (key in keys) {
            if (shouldFetch(key, freshness, loaded[key]?.entry, report = reportSuppression)) toFetch += key
        }
        return toFetch
    }

    /**
     * Fetches [keys] through one [batchFetcher] call (or independent single fetches when none
     * is configured), reusing the per-key single-flight machinery via [startBatch], then awaits
     * each. Returns only the keys that resolved; a key the batch omits, or whose fetch throws,
     * is absent (its error has already surfaced through [AquiferEvents]).
     */
    private suspend fun batchRefresh(keys: Set<K>): Map<K, V> {
        val deferreds = startBatch(keys)
        return buildMap(deferreds.size) {
            for ((key, deferred) in deferreds) {
                try {
                    put(key, deferred.await())
                } catch (cancellation: CancellationException) {
                    // A caller-side cancellation propagates; a store-closure cancellation of the
                    // shared fetch becomes AquiferException, matching get()/close()'s contract.
                    coroutineContext.ensureActive()
                    throw AquiferException("Aquifer was closed during getAll", cancellation)
                } catch (@Suppress("TooGenericExceptionCaught") ignored: Exception) {
                    // This key failed (batch omitted it, or the call threw); its error already
                    // surfaced through AquiferEvents. Omit it — getAll returns the resolved set.
                    // Caught as Exception, not Throwable, so a fatal Error still propagates.
                }
            }
        }
    }

    /**
     * Registers a single-flight fetch for each of [keys] and dispatches the one shared batch
     * call — a [batchFetcher] or a [conditionalBatchFetcher] — (or independent single fetches
     * when neither is configured), **without awaiting** — returning the per-key [Deferred]s.
     * [getAll] (via [batchRefresh]) awaits them for its result map; the fire-and-forget callers
     * ([prefetchAll], [streamMany]'s pre-trigger) ignore them — the fetches still run, commit,
     * and broadcast in the store scope, and a per-key failure is reported through [AquiferEvents]
     * and held in its (un-awaited) deferred. Keys already in flight as single fetches are joined,
     * not re-requested.
     */
    private fun startBatch(keys: Set<K>): Map<K, Deferred<V>> {
        if (keys.isEmpty()) return emptyMap()
        val deferreds = LinkedHashMap<K, Deferred<V>>()
        if (batchFetcher == null && conditionalBatchFetcher == null) {
            for (key in keys) deferreds[key] = refresh(key)
        } else {
            // A CompletableDeferred (not a lazy async, whose await() would start it on the
            // first slice) so the one call dispatches strictly after every slice is registered
            // in `inFlight` — robust even on a multi-threaded dispatcher. The shared result is a
            // per-key FetchResult, so a conditional batch's NotModified rides the same channel.
            val batchResult = CompletableDeferred<Map<K, FetchResult<V>>>()
            // Published by the shared retry loop; each slice reads it so a terminal per-key
            // failure reports the batch's true attempt count through onFetchFailed.
            val batchAttempts = AtomicInteger(1)
            val started = LinkedHashSet<K>()
            for (key in keys) {
                deferreds[key] = refreshWith(key, onStarted = { started += key }) { _, setAttempts ->
                    val result = try {
                        batchResult.await()
                    } finally {
                        // Resolved or threw, surface how many attempts the batch took.
                        setAttempts(batchAttempts.get())
                    }
                    // The slice returns its FetchResult; refreshWith's resolve() handles Fresh
                    // vs NotModified against the key's prior entry, exactly as the single path.
                    result[key] ?: throw BatchKeyMissingException(key)
                }
            }
            // Request exactly the keys whose slice we started; a key that joined an existing
            // single fetch awaits that one, not batchResult, so it must not be in the call.
            if (started.isNotEmpty()) dispatchBatch(started, batchResult, batchAttempts)
        }
        return deferreds
    }

    /**
     * Runs the one shared batch call for [keys] and fans its outcome into [batchResult], which
     * every per-key slice awaits. A failure completes [batchResult] exceptionally so each slice
     * reports it through [failFetch].
     */
    private fun dispatchBatch(
        keys: Set<K>,
        batchResult: CompletableDeferred<Map<K, FetchResult<V>>>,
        attempts: AtomicInteger,
    ) {
        scope.launch {
            try {
                batchResult.complete(fetchBatchWithRetry(keys, attempts))
            } catch (cancellation: CancellationException) {
                batchResult.cancel(cancellation)
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
                batchResult.completeExceptionally(failure)
            }
        }
    }

    /**
     * Runs the shared whole-batch call for [keys] under the store [retry] policy. A retryable
     * transport failure backs off and re-runs the **entire** batch (retry-all): a partial map's
     * omitted keys are definitive misses — [BatchKeyMissingException], not transient errors — so
     * there is no failed slice to re-request, and an omission never triggers a retry. Each retry
     * fires [AquiferEvents.onFetchRetried] for every key in the batch, mirroring the single-key
     * path, and publishes the running attempt count through [attempts] so a terminal failure's
     * [AquiferEvents.onFetchFailed] reports it per key. Cancellation is never retried.
     */
    private suspend fun fetchBatchWithRetry(keys: Set<K>, attempts: AtomicInteger): Map<K, FetchResult<V>> {
        var attempt = 1
        while (true) {
            try {
                return runBatch(keys)
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
                val nextDelay = retry.delayAfter(attempt, failure) ?: throw failure
                for (key in keys) notify { onFetchRetried(key, attempt, failure, nextDelay) }
                attempt++
                attempts.set(attempt)
                delay(nextDelay)
            }
        }
    }

    /**
     * The one shared multi-key backend call, as a per-key [FetchResult]. A
     * [conditionalBatchFetcher] receives each key's cached validator (gathered here from the
     * freshest local entry) and may answer [FetchResult.NotModified] per key; a plain
     * [batchFetcher] receives the key set and returns values, wrapped as [FetchResult.Fresh].
     * Exactly one of the two is configured (the builder enforces it).
     */
    private suspend fun runBatch(keys: Set<K>): Map<K, FetchResult<V>> {
        conditionalBatchFetcher?.let { batch ->
            val loaded = loadAll(keys)
            val validators = LinkedHashMap<K, String?>(keys.size)
            for (key in keys) validators[key] = loaded[key]?.entry?.validator
            return batch(validators)
        }
        val batch = checkNotNull(batchFetcher) { "runBatch requires a batch fetcher" }
        return batch(keys).mapValues { FetchResult.Fresh(it.value) }
    }

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

    override suspend fun putAll(entries: Map<K, V>) {
        checkOpen()
        if (entries.isEmpty()) return
        // One fenced commit for the whole batch, mirroring put. The commit sequences increase in
        // iteration order so collectors arbitrate updates correctly.
        val now = clock.nowMillis()
        val committed = LinkedHashMap<K, MemoryCache.Entry<V>>(entries.size)
        commitGuard.withLock {
            // Persist everything first, like put: if the write throws it propagates before any
            // in-memory commit or broadcast, so the *visible* state (memory + Updated events) is
            // all-or-nothing. writeAll lets a store batch the whole map in one transaction; the
            // default per-key loop is not transactional, so a mid-batch failure can still leave an
            // earlier prefix on disk (it resurfaces on the next load()).
            persistence?.writeAll(entries.mapValues { (_, value) -> PersistedEntry(value, now) })
            // Then the in-memory commits + fences (none of which throw): fence off any in-flight
            // fetch, clear the negative-cache record, and stamp a fresh entry per key.
            for ((key, value) in entries) {
                fence(key)
                negative.remove(key)
                val entry = MemoryCache.Entry(value, now, sequencer.incrementAndGet())
                memory.put(key, entry)
                committed[key] = entry
            }
        }
        // One broadcast per committed key, outside the lock (like put) so a slow collector can't
        // stall it; every memory commit above has a matching emission here.
        for ((key, entry) in committed) {
            events.emit(Event.Updated(key, entry.value, Origin.LOCAL, now, entry.sequence))
        }
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

    override suspend fun invalidateWhere(predicate: (K) -> Boolean) {
        checkOpen()
        // Candidate keys = everything this process currently tracks: resident memory entries,
        // active fetch-capable streams, in-flight fetches, and negative-cache and write-epoch
        // records. A SourceOfTruth can't enumerate its keys, so a key in none of those — a
        // persisted entry evicted from memory and never re-touched, or one never loaded this
        // run — is out of reach (use invalidateAll for that). The predicate runs here, never
        // under commitGuard, so user code can't stall or re-enter the store while it's held.
        val matched = buildSet {
            addAll(memory.keys())
            addAll(activeKeys.keys)
            addAll(inFlight.keys)
            addAll(negative.keys)
            addAll(keyEpochs.keys)
        }.filter(predicate)
        if (matched.isEmpty()) return
        // One fenced commit for the batch, mirroring invalidate per key. Delete persistence for
        // every matched key first, like putAll: a delete failure propagates before any in-memory
        // drop or sequence allocation, so the visible state (memory + Invalidated events) is
        // all-or-nothing. deleteMany lets a store batch the whole set in one transaction; the
        // default per-key loop is not transactional, so a mid-batch failure can leave an earlier
        // prefix deleted on disk. Then the non-throwing in-memory drops + fences; sequences
        // increase in iteration order, and the per-key broadcasts happen outside the lock (like
        // invalidate) so a slow collector can't stall the commit.
        val drops = ArrayList<Pair<K, Long>>(matched.size)
        commitGuard.withLock {
            persistence?.deleteMany(matched)
            for (key in matched) {
                fence(key)
                negative.remove(key)
                memory.remove(key)
                drops += key to sequencer.incrementAndGet()
            }
        }
        for ((key, sequence) in drops) {
            events.emit(Event.Invalidated(key, sequence))
        }
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

    // A non-suspending peek at memory residency: no commitGuard, no I/O, no checkOpen — safe to
    // call from anywhere (a Compose debug overlay, eviction tuning), even on a closed store.
    // memory.keys() snapshots under its own monitor, so the returned set is stable.
    override fun snapshot(): Set<K> = memory.keys()

    // Non-suspending like snapshot(): lock-free counter reads + the inFlight gauge, no checkOpen.
    override fun stats(): CacheStats =
        CacheStats(hits.get(), misses.get(), memory.evictions(), inFlight.size)

    /** Records one caller read as a hit or a miss for [stats]; see [isCacheHit]. */
    private fun recordRead(freshness: Freshness, present: Boolean, usable: Boolean) {
        if (isCacheHit(freshness, present, usable)) hits.incrementAndGet() else misses.incrementAndGet()
    }

    /**
     * Whether a caller read was served from cache without a fetch: the value-bearing strategies
     * count a usable ([CacheFirst]) or merely present ([CacheOnly]/[StaleWhileRevalidate], which
     * serve stale and revalidate in the background) entry as a hit; the network-priority ones
     * ([NetworkFirst]/[NetworkOnly]) always go to the network, so they are always misses.
     */
    private fun isCacheHit(freshness: Freshness, present: Boolean, usable: Boolean): Boolean =
        when (freshness) {
            Freshness.CacheOnly, Freshness.StaleWhileRevalidate -> present
            Freshness.CacheFirst -> usable
            Freshness.NetworkFirst, Freshness.NetworkOnly -> false
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
            val stale = entry == null ||
                isExpired(key, entry.writtenAtMillis, entryFreshFor = entry.serverFreshForMillis?.milliseconds)
            if (stale && suppression(key) == null) refresh(key)
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
                    snapshot.entry.serverFreshForMillis,
                ),
            )
        }
        // A fetch that started before this collector subscribed broadcast its Fetching event
        // too early for us to see it. Note it now (before refresh, so a fetch we start
        // ourselves is not mistaken for a pre-existing one) and replay it below.
        val joinedInFlightFetch = inFlight.containsKey(key)
        val needsValue = entry == null ||
            isExpired(key, entry.writtenAtMillis, maxAge, entry.serverFreshForMillis?.milliseconds)
        recordRead(freshness, present = entry != null, usable = entry != null && !needsValue)
        val wantsFetch = wantsFetch(freshness, needsValue)
        val block = fetchBlock(key, wantsFetch, freshness)
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
     * The negative-cache entry that holds back an otherwise-wanted fetch of [key], or `null`
     * when a fetch may proceed. Consults the suppression memory *only* when [wantsFetch] is true
     * and the strategy isn't [Freshness.NetworkOnly] — the explicit-demand carve-out — so no-op
     * decision paths never spuriously report a suppression. The single home for this gating,
     * shared by [prime], [prefetch], and [keysNeedingFetch].
     *
     * [report] true reports through [AquiferEvents.onFetchSuppressed] (the default, for the
     * paths that gate a real fetch); `false` uses the non-notifying [activeSuppression], for
     * streamMany's pre-batch gate whose per-key [prime] reports instead.
     */
    private fun fetchBlock(key: K, wantsFetch: Boolean, freshness: Freshness, report: Boolean = true): NegativeEntry? =
        if (wantsFetch && freshness != Freshness.NetworkOnly) {
            if (report) suppression(key) else activeSuppression(key)
        } else {
            null
        }

    /**
     * Returns the in-flight fetch for [key], starting one when none is running. The fetch runs
     * in the store's scope so it completes — and lands in the cache — even if every caller that
     * was awaiting it has been cancelled. Progress and outcome are broadcast on the bus.
     */
    private fun refresh(key: K): Deferred<V> = refreshWith(key) { prior, setAttempts ->
        fetchWithRetry(key, prior?.validator, setAttempts)
    }

    /**
     * Starts (or joins) a single-flight fetch for [key], with [transport] supplying the value.
     * The single-key path retries via [fetchWithRetry]; a batch ([getAll]) passes a transport
     * that awaits a shared multi-key call. Everything around the transport — single-flight
     * dedup, epoch fencing, the commit, negative-cache, persistence, and `Fetching`/`Updated`/
     * `Failed` events — is identical for both, so batching changes only the wire call.
     *
     * [onStarted] fires only when this call wins the registration race and starts a *new*
     * fetch (not when it joins one already in flight) — [getAll] uses it to batch exactly the
     * keys it actually started, never a key that joined an existing single fetch.
     */
    private fun refreshWith(
        key: K,
        onStarted: (() -> Unit)? = null,
        transport: suspend (prior: MemoryCache.Entry<V>?, setAttempts: (Int) -> Unit) -> FetchResult<V>,
    ): Deferred<V> {
        inFlight[key]?.let { return it }
        // Captured here — before the fetch is registered in inFlight below — not inside the
        // lazily-started body. The body doesn't run until pending.start(), which happens after
        // putIfAbsent; a fence (epoch bump + inFlight eviction) landing in that gap would
        // otherwise be read by the body as the *current* epoch, so the fetch's commit would pass
        // its epoch check and clobber the very write that fenced it. Capturing at registration
        // only ever fails safe: any mutation after this point leaves this epoch stale, so the
        // commit is correctly dropped.
        val epoch = epochOf(key)
        val pending = scope.async(start = CoroutineStart.LAZY) {
            var attempts = 1
            try {
                events.emit(Event.Fetching(key))
                notify { onFetchStarted(key) }
                // The validator — and the value a NotModified resolves to — come from the
                // entry as it stood when the fetch started; a mutation during the fetch
                // fences the commit regardless. Plain stores skip this read entirely.
                val prior = if (conditional) load(key)?.entry else null
                val startedAt = clock.nowMillis()
                val result = transport(prior) { attempts = it }
                val now = clock.nowMillis()
                notify { onFetchSucceeded(key, (now - startedAt).milliseconds) }
                val resolved = resolve(key, result, prior)
                commitFetched(key, epoch, resolved, now)
                resolved.value
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (@Suppress("TooGenericExceptionCaught") failure: Throwable) {
                failFetch(key, epoch, failure, attempts)
                throw failure
            }
        }
        val existing = inFlight.putIfAbsent(key, pending)
        if (existing != null) {
            // Lost the race: another caller registered a fetch first. Join theirs.
            pending.cancel()
            return existing
        }
        onStarted?.invoke()
        pending.invokeOnCompletion { inFlight.remove(key, pending) }
        pending.start()
        return pending
    }

    /**
     * Commits a freshly-fetched [value] for [key] when its [epoch] is still current: clears
     * the negative-cache record, writes memory and persistence, and broadcasts `Updated`. A
     * mutation that raced the fetch leaves the epoch moved, and the commit is dropped.
     */
    private suspend fun commitFetched(key: K, epoch: Pair<Long, Long>, resolved: Resolved<V>, now: Long) {
        val committed: MemoryCache.Entry<V>? = commitGuard.withLock {
            if (epochOf(key) == epoch) {
                // Cleared with the commit it celebrates: a read between commit and a later
                // clear could otherwise still see the stale suppression window.
                negative.remove(key)
                val entry = MemoryCache.Entry(
                    resolved.value,
                    now,
                    sequencer.incrementAndGet(),
                    resolved.validator,
                    resolved.serverFreshForMillis,
                )
                memory.put(key, entry)
                persistFetched(key, resolved.value, now, resolved.validator, resolved.serverFreshForMillis)
                entry
            } else {
                null
            }
        }
        // Re-check before broadcasting: a mutation may have raced the gap above, and observers
        // should not see a fenced-off value even transiently.
        if (committed != null && epochOf(key) == epoch) {
            events.emit(
                Event.Updated(
                    key,
                    resolved.value,
                    Origin.FETCHER,
                    now,
                    committed.sequence,
                    committed.serverFreshForMillis,
                ),
            )
        }
    }

    /**
     * Records and broadcasts a terminal fetch [failure] for [key] when its [epoch] is current.
     * The epoch check and the negative-cache record commit together under [commitGuard], so a
     * racing put/invalidate can't have its just-cleared failure memory re-poisoned by a
     * failure that observed the pre-mutation epoch.
     */
    private suspend fun failFetch(key: K, epoch: Pair<Long, Long>, failure: Throwable, attempts: Int) {
        notify { onFetchFailed(key, failure, attempts) }
        val current = commitGuard.withLock {
            (epochOf(key) == epoch).also { if (it) recordFailure(key, failure) }
        }
        if (current) {
            events.emit(Event.Failed(key, failure))
        }
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
                            persisted.serverFreshForMillis,
                        )
                    memory.put(key, entry)
                    Snapshot(entry, Origin.PERSISTENCE)
                }

                else -> null
            }
        }
    }

    /**
     * The batched twin of [load] for the multi-key read paths ([getAll], [keysNeedingFetch],
     * [runBatch]): memory hits resolve without I/O, and every memory miss is read from the source
     * of truth in a single [SourceOfTruth.readAll] call instead of N. Each miss is fenced exactly
     * as [load] — its epoch is captured before the batched read and re-checked under [commitGuard]
     * with a memory re-read — so a put/invalidate racing the read neither resurrects a deleted
     * entry nor overwrites a fresher commit. Keys with nothing cached are absent from the result.
     */
    private suspend fun loadAll(keys: Collection<K>): Map<K, Snapshot<V>> {
        val result = LinkedHashMap<K, Snapshot<V>>(keys.size)
        val store = persistence
        // Epoch snapshot per memory-miss key, captured before the batched read like load() does.
        val epochs = if (store == null) null else LinkedHashMap<K, Pair<Long, Long>>()
        for (key in keys) {
            val cached = memory.get(key)
            when {
                cached != null -> result[key] = Snapshot(cached, Origin.MEMORY)
                store != null -> epochs!![key] = epochOf(key)
            }
        }
        if (store == null || epochs!!.isEmpty()) return result
        val persisted = store.readAll(epochs.keys)
        if (persisted.isEmpty()) return result
        commitGuard.withLock {
            for ((key, entry) in persisted) {
                val epoch = epochs[key] ?: continue // a store returning an unrequested key: ignore it
                val existing = memory.get(key)
                when {
                    existing != null -> result[key] = Snapshot(existing, Origin.MEMORY)

                    epochOf(key) == epoch -> {
                        val hydrated =
                            MemoryCache.Entry(
                                entry.value,
                                entry.writtenAtMillis,
                                sequencer.incrementAndGet(),
                                entry.validator,
                                entry.serverFreshForMillis,
                            )
                        memory.put(key, hydrated)
                        result[key] = Snapshot(hydrated, Origin.PERSISTENCE)
                    }
                    // else: the epoch moved (a put/invalidate landed during the read) — omit the
                    // key so a deleted entry is never resurrected, exactly as load() does.
                }
            }
        }
        return result
    }

    /**
     * Best-effort write-through after a successful fetch: a storage failure must not turn a
     * successful fetch into an error (the value is already cached and broadcast), so anything
     * but cancellation is swallowed. Direct mutations ([put], [invalidate]) propagate instead.
     */
    private suspend fun persistFetched(
        key: K,
        value: V,
        writtenAtMillis: Long,
        validator: String?,
        serverFreshForMillis: Long?,
    ) {
        val store = persistence ?: return
        try {
            store.write(key, PersistedEntry(value, writtenAtMillis, validator, serverFreshForMillis))
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

    /** What a resolved fetch commits: the value, its validator, and its server-declared lifetime. */
    internal data class Resolved<V : Any>(
        val value: V,
        val validator: String?,
        val serverFreshForMillis: Long?,
    )

    /**
     * Resolves what a fetch [result] commits: a [FetchResult.Fresh] carries its own value,
     * validator, and (server-derived) [FetchResult.Fresh.freshFor] as whole millis; a
     * [FetchResult.NotModified] re-commits the [prior] entry — "still current" only means
     * something against a cached entry, whose age the commit resets, so the prior entry's
     * server lifetime carries forward and re-ages off the new write time.
     */
    private fun resolve(key: K, result: FetchResult<V>, prior: MemoryCache.Entry<V>?): Resolved<V> =
        when (result) {
            is FetchResult.Fresh ->
                Resolved(result.value, result.validator, result.freshFor?.inWholeMilliseconds)
            is FetchResult.NotModified -> {
                val entry = prior
                // A prior entry alone is not enough: one without a validator (a local put,
                // or persisted before validators existed) never sent a revalidation token,
                // so there is nothing a NotModified could be "not modified" against.
                check(entry != null && entry.validator != null) {
                    "Fetcher returned NotModified for '$key' but was given no validator " +
                        "(nothing cached, or the cached entry carries none)"
                }
                Resolved(entry.value, entry.validator, entry.serverFreshForMillis)
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
                // A coalescing batch store routes single-key fetches through the accumulator
                // (a retry re-enters the next window); everyone else calls the fetcher directly.
                return accumulator?.let { FetchResult.Fresh(it.load(key)) } ?: fetcher(key, validator)
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
     * Staleness as the highest-precedence freshness horizon that applies:
     *
     * 1. [maxAge] — the per-call override, the caller's explicit bar, never jittered.
     * 2. [entryFreshFor] — the entry's server-declared remaining lifetime (an origin
     *    `Cache-Control: max-age`), authoritative over the store TTL and likewise never
     *    jittered; [Duration.ZERO] means immediately stale, `null` means no server opinion.
     * 3. the store-wide TTL with the entry's deterministic jitter.
     *
     * With no [maxAge] and no [entryFreshFor] this reduces exactly to the pre-existing
     * `maxAge ?: jitteredTimeToLive`, so behaviour is unchanged for entries that carry neither.
     */
    private fun isExpired(
        key: K,
        writtenAtMillis: Long,
        maxAge: Duration? = null,
        entryFreshFor: Duration? = null,
    ): Boolean {
        val horizon = maxAge ?: entryFreshFor ?: jitteredTimeToLive(key, writtenAtMillis)
        // Duration arithmetic, not inWholeMilliseconds: a sub-millisecond horizon must not
        // truncate to zero and declare everything instantly stale. INFINITE compares false
        // against any finite elapsed time, giving "always fresh" naturally.
        return (clock.nowMillis() - writtenAtMillis).milliseconds >= horizon
    }

    /** This entry's effective TTL: scaled by a stable factor of its key and write time. */
    private fun jitteredTimeToLive(key: K, writtenAtMillis: Long): Duration {
        if (ttlJitter == 0.0 || timeToLive == Duration.INFINITE) return timeToLive
        return timeToLive * (1.0 - ttlJitter * TtlJitter.fractionFor(writtenAtMillis, key.hashCode()))
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
