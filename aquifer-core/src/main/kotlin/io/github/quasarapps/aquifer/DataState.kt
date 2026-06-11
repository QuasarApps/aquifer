package io.github.quasarapps.aquifer

/**
 * A snapshot of the data associated with a key, as observed through [Aquifer.stream].
 *
 * Streams emit a new [DataState] whenever something relevant happens to the observed key:
 * a value is served from cache, a fetch starts, a fetch succeeds or fails, or another caller
 * writes or invalidates the key.
 *
 * All states expose [value], the most recent value known for the key, so UIs can keep
 * rendering data while a refresh is in flight or after a refresh has failed:
 *
 * ```
 * aquifer.stream(userId).collect { state ->
 *     state.value?.let(::render)
 *     spinner.isVisible = state is DataState.Loading
 *     if (state is DataState.Failure) showRefreshError(state.error)
 * }
 * ```
 */
public sealed interface DataState<out V : Any> {

    /** The most recent value known for the key, or `null` if no value is available yet. */
    public val value: V?

    /**
     * A fetch is in flight for the observed key.
     *
     * [value] carries the previous value, if one was known when the fetch started, so a UI
     * can show existing content together with a refresh indicator instead of a blank screen.
     */
    public data class Loading<out V : Any>(
        override val value: V? = null,
    ) : DataState<V>

    /**
     * A value is available for the observed key.
     *
     * @property value the value itself.
     * @property origin where this value came from; see [Origin].
     * @property isStale `true` when the value has outlived the configured time-to-live at the
     *   moment it was emitted. A stale value is still usable — depending on the requested
     *   [Freshness], a revalidation may already be in flight.
     */
    public data class Content<out V : Any>(
        override val value: V,
        val origin: Origin,
        val isStale: Boolean = false,
    ) : DataState<V>

    /**
     * A fetch for the observed key failed.
     *
     * [value] carries the last known value, if any, so UIs can keep showing usable (possibly
     * stale) data alongside the error. Failures are broadcast to every active stream of the
     * key, regardless of which caller triggered the fetch.
     *
     * @property error the exception thrown by the fetcher (or a [CacheMissException] when a
     *   [Freshness.CacheOnly] stream found nothing in the cache).
     */
    public data class Failure<out V : Any>(
        val error: Throwable,
        override val value: V? = null,
    ) : DataState<V>
}

/** Identifies where a [DataState.Content] value came from. */
public enum class Origin {
    /** Served from the in-memory cache. */
    MEMORY,

    /** Read from the configured [SourceOfTruth] (typically disk). */
    PERSISTENCE,

    /** Returned by the fetcher (typically the network). */
    FETCHER,

    /** Written locally through [Aquifer.put]. */
    LOCAL,
}
