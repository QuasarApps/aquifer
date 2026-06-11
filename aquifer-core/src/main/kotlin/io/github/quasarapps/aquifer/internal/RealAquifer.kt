package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Aquifer
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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onSubscription
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

internal class RealAquifer<K : Any, V : Any>(
    private val fetcher: suspend (key: K) -> V,
    private val timeToLive: Duration,
    maxEntries: Int,
    private val clock: WallClock,
    parentScope: CoroutineScope?,
    private val persistence: SourceOfTruth<K, V>? = null,
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
            // Last value this collector has seen; backs the `value` field of Loading/Failure.
            var last: V? = null
            events
                .onSubscription { prime(key, freshness) }
                .buffer(Channel.UNLIMITED)
                .flowOn(busDrainContext)
                .collect { event ->
                    when (event) {
                        is Event.Updated -> if (event.key == key) {
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
                            if (freshness != Freshness.CacheOnly) refresh(key)
                        }

                        is Event.ClearedAll -> {
                            last = null
                            if (freshness != Freshness.CacheOnly) refresh(key)
                        }
                    }
                }
        }.distinctUntilChanged()
    }

    override suspend fun get(key: K, freshness: Freshness): V {
        checkOpen()
        val entry = load(key)?.entry
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

            Freshness.NetworkOnly -> refresh(key).await()
        }
    }

    override suspend fun fresh(key: K): V = get(key, Freshness.NetworkOnly)

    override suspend fun put(key: K, value: V) {
        checkOpen()
        val now = clock.nowMillis()
        // Persist first so a storage failure propagates before anything becomes visible;
        // direct mutations are all-or-nothing, unlike best-effort fetch write-through.
        persistence?.write(key, PersistedEntry(value, now))
        memory.put(key, MemoryCache.Entry(value, now))
        events.emit(Event.Updated(key, value, Origin.LOCAL, now))
    }

    override suspend fun invalidate(key: K) {
        checkOpen()
        persistence?.delete(key)
        memory.remove(key)
        events.emit(Event.Invalidated(key))
    }

    override suspend fun invalidateAll() {
        checkOpen()
        persistence?.deleteAll()
        memory.clear()
        events.emit(Event.ClearedAll)
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            job.cancel()
        }
    }

    /**
     * Emits the initial snapshot for a new stream collector and triggers a fetch when the
     * requested freshness calls for one. Runs inside [onSubscription], i.e. after the
     * collector is registered with the bus, so no update can slip between the snapshot read
     * and live event delivery.
     */
    private suspend fun FlowCollector<Event<K, V>>.prime(key: K, freshness: Freshness) {
        val snapshot = load(key)
        val entry = snapshot?.entry
        if (snapshot != null && freshness != Freshness.NetworkOnly) {
            emit(Event.Updated(key, snapshot.entry.value, snapshot.origin, snapshot.entry.writtenAtMillis))
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
            try {
                events.emit(Event.Fetching(key))
                val value = fetcher(key)
                val now = clock.nowMillis()
                memory.put(key, MemoryCache.Entry(value, now))
                events.emit(Event.Updated(key, value, Origin.FETCHER, now))
                persistFetched(key, value, now)
                value
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (failure: Throwable) {
                events.emit(Event.Failed(key, failure))
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
     */
    private suspend fun load(key: K): Snapshot<V>? {
        memory.get(key)?.let { return Snapshot(it, Origin.MEMORY) }
        val store = persistence ?: return null
        val persisted = store.read(key) ?: return null
        val entry = MemoryCache.Entry(persisted.value, persisted.writtenAtMillis)
        memory.put(key, entry)
        return Snapshot(entry, Origin.PERSISTENCE)
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
        } catch (_: Throwable) {
            // Swallowed by design; observability hooks for storage failures are on the roadmap.
        }
    }

    private data class Snapshot<V : Any>(
        val entry: MemoryCache.Entry<V>,
        val origin: Origin,
    )

    /** Awaits the shared fetch, falling back to [fallback] when the fetch fails (stale-if-error). */
    private suspend fun awaitFetch(key: K, fallback: V?): V {
        try {
            return refresh(key).await()
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (failure: Throwable) {
            return fallback ?: throw failure
        }
    }

    private fun isExpired(writtenAtMillis: Long): Boolean {
        if (timeToLive == Duration.INFINITE) return false
        return clock.nowMillis() - writtenAtMillis >= timeToLive.inWholeMilliseconds
    }

    private fun checkOpen() = check(!closed.get()) { "Aquifer is closed" }

    private companion object {
        const val EVENT_BUFFER_CAPACITY = 64
    }
}
