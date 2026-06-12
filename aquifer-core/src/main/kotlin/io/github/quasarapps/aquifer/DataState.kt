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
     * @property error the exception thrown by the fetcher.
     */
    public data class Failure<out V : Any>(
        val error: Throwable,
        override val value: V? = null,
    ) : DataState<V>

    /**
     * The store affirmatively has no value for the observed key, and this stream's strategy
     * will not fetch one.
     *
     * Emitted only to [Freshness.CacheOnly] streams: on initial collection when nothing is
     * cached, and when the key is dropped by [Aquifer.invalidate] or [Aquifer.invalidateAll]
     * while the stream is active — so a cache-only screen observes a logout-style reset
     * instead of rendering deleted data forever. Fetch-capable streams never emit it; their
     * post-invalidation refetch communicates the same transition as a [Loading] whose
     * [value] is `null`.
     *
     * This completes the "no value" vocabulary, each shape honest about why: [Loading]
     * (work in flight), [Failure] (work failed), `Empty` (nothing there, nothing happening).
     * Render it as your genuine empty state. Designed in
     * [RFC #23](https://github.com/QuasarApps/aquifer/issues/23).
     */
    public data object Empty : DataState<Nothing> {
        override val value: Nothing? get() = null
    }
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
