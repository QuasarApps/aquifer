package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Origin

/**
 * Internal change notifications broadcast to active streams. Every mutation of a key's data —
 * from fetches, local writes, or invalidations — flows through exactly one of these events,
 * which is what keeps all observers of a key coherent.
 */
internal sealed interface Event<out K : Any, out V : Any> {

    /** A new value was cached for [key]. */
    data class Updated<K : Any, V : Any>(
        val key: K,
        val value: V,
        val origin: Origin,
        val writtenAtMillis: Long,
    ) : Event<K, V>

    /** A fetch actually started for [key] (emitted once per shared in-flight fetch). */
    data class Fetching<K : Any>(val key: K) : Event<K, Nothing>

    /** The in-flight fetch for [key] failed with [error]. */
    data class Failed<K : Any>(val key: K, val error: Throwable) : Event<K, Nothing>

    /** The cached entry for [key] was dropped. */
    data class Invalidated<K : Any>(val key: K) : Event<K, Nothing>

    /** All cached entries were dropped. */
    data object ClearedAll : Event<Nothing, Nothing>
}
