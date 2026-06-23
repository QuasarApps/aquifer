package io.github.quasarapps.aquifer.test

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

/**
 * Builds a [FakeAquifer] — a programmable, in-memory [Aquifer] for unit-testing repositories, the
 * test-suite sibling of `previewAquifer`. Unlike a preview store it *fetches*: you script per-key
 * values, failures, and delays (or a default [fetcher]), then assert how many times each key was
 * fetched.
 *
 * ```
 * val users = fakeAquifer<String, User>(backgroundScope) {
 *     seed("ada" to User("ada"))           // already cached, no fetch
 *     returns("grace", User("grace"))      // fetched on demand
 *     failsWith("bad", IOException())      // fetch fails
 *     fetcher { id -> User(id) }           // fallback for any other key
 * }
 *
 * assertEquals(User("grace"), users.get("grace"))
 * assertEquals(1, users.fetchCount("grace"))
 * assertEquals(0, users.fetchCount("ada")) // served from the seed
 * ```
 *
 * Fetches run with the requested [Freshness] over a no-TTL in-memory cache (a cached value is
 * always considered fresh until overwritten or invalidated, so `maxAge` is validated but has no
 * effect — pair the real store with [FakeClock] for staleness tests). [prefetch]/[prefetchAll]
 * fetch on [scope]; collect [stream]/[streamMany] within `runTest` and use [settle] to let that
 * background work run.
 */
public fun <K : Any, V : Any> fakeAquifer(
    scope: CoroutineScope,
    configure: FakeAquiferBuilder<K, V>.() -> Unit = {},
): FakeAquifer<K, V> {
    val builder = FakeAquiferBuilder<K, V>().apply(configure)
    return FakeAquifer(
        scope = scope,
        initialSeed = builder.seed.toMap(),
        initialScripted = builder.scripted.toMap(),
        initialDelays = builder.delays.toMap(),
        initialFetcher = builder.defaultFetcher,
        initialFetchDelay = builder.fetchDelay,
    )
}

/** A scripted fetch response for a key. */
internal sealed interface Scripted<out V : Any> {
    data class Value<out V : Any>(val value: V) : Scripted<V>
    data class Failure(val error: Throwable) : Scripted<Nothing>
}

/** A scripted fetch delay must be non-negative and finite, so a fetch can't be silently
 *  un-delayed or hang forever. */
internal fun requireValidDelay(delay: Duration) {
    require(delay >= Duration.ZERO && delay.isFinite()) {
        "fetch delay must be non-negative and finite, was $delay"
    }
}

/** Configuration DSL for [fakeAquifer]; every setting also has a runtime twin on [FakeAquifer]. */
public class FakeAquiferBuilder<K : Any, V : Any> internal constructor() {

    internal val seed = mutableMapOf<K, V>()
    internal val scripted = mutableMapOf<K, Scripted<V>>()
    internal val delays = mutableMapOf<K, Duration>()
    internal var defaultFetcher: (suspend (K) -> V)? = null

    /** A delay applied to every fetch without a more specific [delays] entry. Default: none. */
    public var fetchDelay: Duration = Duration.ZERO
        set(value) {
            requireValidDelay(value)
            field = value
        }

    /** Pre-populates the cache without counting as a fetch — the equivalent of a warm start. */
    public fun seed(vararg entries: Pair<K, V>) {
        seed.putAll(entries)
    }

    /** The fallback fetch behavior for any key without a scripted [returns]/[failsWith]. */
    public fun fetcher(block: suspend (K) -> V) {
        defaultFetcher = block
    }

    /** Scripts fetching [key] to succeed with [value]. */
    public fun returns(key: K, value: V) {
        scripted[key] = Scripted.Value(value)
    }

    /** Scripts fetching [key] to fail with [error]. */
    public fun failsWith(key: K, error: Throwable) {
        scripted[key] = Scripted.Failure(error)
    }

    /** Scripts fetching [key] to take [duration] (virtual time under `runTest`); must be
     *  non-negative and finite. */
    public fun delays(key: K, duration: Duration) {
        requireValidDelay(duration)
        delays[key] = duration
    }
}

/**
 * A programmable in-memory [Aquifer] for tests; create it with [fakeAquifer].
 *
 * Beyond the [Aquifer] surface it exposes fetch-count assertions ([fetchCount], [fetchedKeys]) and
 * runtime re-scripting ([returns]/[failsWith]/[delays]/[fetcher]) so a test can change a key's
 * response between calls (e.g. fail once, then succeed).
 *
 * Simplifications relative to a real store, all in service of determinism: no time-to-live (cached
 * values never go stale on their own, so `maxAge` is inert); [stream] fetches once, on a miss, for
 * any fetch-capable strategy — the network-priority strategies' force-refetch and cache-bypass are
 * one-shot-read behaviors (get/fresh/getAll), not modeled in streams — then mirrors cache changes,
 * without auto-revalidating on invalidation and reporting every [DataState.Content] as
 * [Origin.MEMORY]; [revalidateActive]/[revalidateOn] are no-ops (the fake tracks no active
 * collectors). There is also no single-flight de-duplication: each read fetches independently, so
 * two *concurrent* loads of the same missing key each run (and each increment [fetchCount]), where
 * the real store collapses them into one — assert fetch counts on sequential calls, or against the
 * real store for single-flight. Because the fake runs one-shot fetches in the caller's coroutine
 * (only [prefetch]/[prefetchAll] use [scope]), [close] marks the store closed so later calls throw
 * [IllegalStateException] — except [snapshot], which stays callable — but cannot cancel in-flight
 * fetches or end active [stream] collectors; test the real store for lifecycle cancellation.
 */
@Suppress("TooManyFunctions")
public class FakeAquifer<K : Any, V : Any> internal constructor(
    private val scope: CoroutineScope,
    initialSeed: Map<K, V>,
    initialScripted: Map<K, Scripted<V>>,
    initialDelays: Map<K, Duration>,
    initialFetcher: (suspend (K) -> V)?,
    initialFetchDelay: Duration,
) : Aquifer<K, V> {

    private val cache = MutableStateFlow(initialSeed)
    private val closed = AtomicBoolean(false)

    private val lock = Any()
    private val scripted = initialScripted.toMutableMap()
    private val perKeyDelay = initialDelays.toMutableMap()
    private var defaultFetcher = initialFetcher
    private var fetchDelay = initialFetchDelay
    private val counts = mutableMapOf<K, Int>()
    private val fetchLog = mutableListOf<K>()

    // --- assertions -------------------------------------------------------------------------

    /**
     * Total number of fetches across all keys (each scripted [returns]/[failsWith]/fetcher call
     * counts). Note there's no single-flight de-duplication — concurrent loads of the same key
     * each count; see the class docs.
     */
    public fun fetchCount(): Int = synchronized(lock) { fetchLog.size }

    /** Number of times [key] has been fetched. */
    public fun fetchCount(key: K): Int = synchronized(lock) { counts[key] ?: 0 }

    /** Every fetched key, in the order the fetches were started (duplicates included). */
    public fun fetchedKeys(): List<K> = synchronized(lock) { fetchLog.toList() }

    /** Clears the recorded fetch counts and log, leaving cache and scripting untouched. */
    public fun resetFetchCounts() {
        synchronized(lock) {
            counts.clear()
            fetchLog.clear()
        }
    }

    // --- runtime re-scripting ---------------------------------------------------------------

    /** Re-scripts [key] to fetch-return [value] from now on. */
    public fun returns(key: K, value: V) {
        synchronized(lock) { scripted[key] = Scripted.Value(value) }
    }

    /** Re-scripts [key] to fail its fetch with [error] from now on. */
    public fun failsWith(key: K, error: Throwable) {
        synchronized(lock) { scripted[key] = Scripted.Failure(error) }
    }

    /** Re-scripts [key]'s fetch delay from now on; must be non-negative and finite. */
    public fun delays(key: K, duration: Duration) {
        requireValidDelay(duration)
        synchronized(lock) { perKeyDelay[key] = duration }
    }

    /** Replaces the fallback [fetcher] from now on. */
    public fun fetcher(block: suspend (K) -> V) {
        synchronized(lock) { defaultFetcher = block }
    }

    // --- Aquifer: reads ---------------------------------------------------------------------

    override fun stream(key: K, freshness: Freshness, maxAge: Duration?): Flow<DataState<V>> {
        checkOpen() // fail fast at call time, like the real store (and again on collection below)
        requireValidMaxAge(maxAge)
        return flow {
            checkOpen()
            val cached = cache.value[key]
            // A stream fetches once, on a miss, for any fetch-capable strategy — so it never
            // surfaces a stale cached value in Loading/Failure. The network-priority strategies'
            // force-refetch and cache-bypass are one-shot-read behaviors (get/fresh/getAll), not
            // modeled here; a stream over an already-cached key just projects it.
            if (freshness != Freshness.CacheOnly && cached == null) {
                emit(DataState.Loading())
                try {
                    doFetch(key)
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (@Suppress("TooGenericExceptionCaught") error: Throwable) {
                    emit(DataState.Failure(error, cache.value[key]))
                }
            }
            emitAll(
                cache
                    .map { it[key] }
                    .distinctUntilChanged()
                    .mapNotNull { value ->
                        when {
                            value != null -> DataState.Content(value, Origin.MEMORY, isStale = false)
                            freshness == Freshness.CacheOnly -> DataState.Empty
                            else -> null // fetch-capable streams stay on their last state, never Empty
                        }
                    },
            )
        }.distinctUntilChanged()
    }

    override fun streamMany(keys: Set<K>, freshness: Freshness): Flow<Map<K, DataState<V>>> {
        checkOpen() // before the empty-keys early return, matching the real store
        if (keys.isEmpty()) return flowOf(emptyMap())
        val ordered = keys.toList()
        return combine(ordered.map { key -> stream(key, freshness) }) { states ->
            buildMap(ordered.size) { ordered.forEachIndexed { index, key -> put(key, states[index]) } }
        }
    }

    override suspend fun get(key: K, freshness: Freshness, maxAge: Duration?): V {
        checkOpen()
        requireValidMaxAge(maxAge)
        return resolve(key, freshness)
    }

    override suspend fun fresh(key: K): V = get(key, Freshness.NetworkOnly)

    override suspend fun getAll(keys: Set<K>, freshness: Freshness): Map<K, V> {
        checkOpen()
        return buildMap(keys.size) {
            for (key in keys) {
                val value = try {
                    resolve(key, freshness)
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (@Suppress("TooGenericExceptionCaught") ignored: Throwable) {
                    null // per-key failure is omitted from the result, exactly like the real getAll
                }
                if (value != null) put(key, value)
            }
        }
    }

    override fun prefetch(key: K, freshness: Freshness) {
        checkOpen()
        if (freshness == Freshness.CacheOnly) return
        scope.launch { prefetchOne(key, freshness) }
    }

    override fun prefetchAll(keys: Set<K>, freshness: Freshness) {
        checkOpen()
        if (freshness == Freshness.CacheOnly || keys.isEmpty()) return
        scope.launch { for (key in keys) prefetchOne(key, freshness) }
    }

    // --- Aquifer: writes & introspection ----------------------------------------------------

    override suspend fun put(key: K, value: V) {
        checkOpen()
        cache.update { it + (key to value) }
    }

    override suspend fun putAll(entries: Map<K, V>) {
        checkOpen()
        if (entries.isEmpty()) return
        cache.update { it + entries }
    }

    override suspend fun invalidate(key: K) {
        checkOpen()
        cache.update { it - key }
    }

    override suspend fun invalidateWhere(predicate: (K) -> Boolean) {
        checkOpen()
        cache.update { current -> current.filterKeys { !predicate(it) } }
    }

    override suspend fun invalidateAll() {
        checkOpen()
        cache.value = emptyMap()
    }

    override fun snapshot(): Set<K> = cache.value.keys.toSet()

    override suspend fun revalidateActive() {
        checkOpen() // no-op: the fake tracks no active collectors; drive refreshes with fresh()/get
    }

    override fun revalidateOn(trigger: Flow<*>) {
        checkOpen() // no-op, for the same reason as revalidateActive
    }

    override fun close() {
        closed.set(true)
    }

    // --- internals --------------------------------------------------------------------------

    private suspend fun resolve(key: K, freshness: Freshness): V {
        val cached = cache.value[key]
        return when (freshness) {
            Freshness.CacheOnly -> cached ?: throw CacheMissException(key)
            Freshness.NetworkOnly -> doFetch(key)
            Freshness.CacheFirst, Freshness.StaleWhileRevalidate -> cached ?: doFetch(key)
            Freshness.NetworkFirst -> fetchOrFallBackTo(key, cached)
        }
    }

    /** Fetches [key]; on failure serves [cached] if present, else rethrows — NetworkFirst. */
    private suspend fun fetchOrFallBackTo(key: K, cached: V?): V = try {
        doFetch(key)
    } catch (cancellation: CancellationException) {
        throw cancellation
    } catch (@Suppress("TooGenericExceptionCaught") error: Throwable) {
        cached ?: throw error
    }

    /** The prefetch fetch decision, given the strategy and cache presence — mirrors get(). */
    private fun initialFetch(freshness: Freshness, hasCached: Boolean): Boolean = when (freshness) {
        Freshness.CacheOnly -> false
        // Network-priority strategies fetch regardless of what's cached, exactly as get() decides.
        Freshness.NetworkOnly, Freshness.NetworkFirst -> true
        Freshness.CacheFirst, Freshness.StaleWhileRevalidate -> !hasCached
    }

    private suspend fun prefetchOne(key: K, freshness: Freshness) {
        if (!initialFetch(freshness, cache.value[key] != null)) return
        try {
            doFetch(key)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (@Suppress("TooGenericExceptionCaught") ignored: Throwable) {
            // fire-and-forget: a prefetch failure is recorded as a fetch but never thrown
        }
    }

    /** Records the fetch, applies the scripted delay, then returns the value or throws. */
    private suspend fun doFetch(key: K): V {
        val (response, wait, fetcher) = synchronized(lock) {
            counts[key] = (counts[key] ?: 0) + 1
            fetchLog.add(key)
            Triple(scripted[key], perKeyDelay[key] ?: fetchDelay, defaultFetcher)
        }
        if (wait > Duration.ZERO) delay(wait)
        val value = when (response) {
            is Scripted.Value -> response.value
            is Scripted.Failure -> throw response.error
            null -> fetcher?.invoke(key) ?: error(
                "FakeAquifer: no response scripted for key=$key. " +
                    "Configure returns(key, …), failsWith(key, …), or fetcher { … }.",
            )
        }
        cache.update { it + (key to value) }
        return value
    }

    private fun checkOpen() {
        check(!closed.get()) { "This FakeAquifer has been closed" }
    }

    /** The fake has no clock or TTL, so `maxAge` has no effect — but the contract is enforced. */
    private fun requireValidMaxAge(maxAge: Duration?) {
        require(maxAge == null || maxAge.isPositive()) { "maxAge must be positive, was $maxAge" }
    }
}
