package io.github.quasarapps.aquifer

/**
 * A point-in-time snapshot of a store's cache counters — the aggregate numbers [AquiferEvents]
 * can't give you. Obtained from [Aquifer.stats]; see there for precisely what is counted.
 *
 * @property hits caller reads served from the cache without awaiting a fetch.
 * @property misses caller reads with no usable cached value, which went to the network (or, for
 *   [Freshness.CacheOnly], found nothing).
 * @property evictions entries dropped from the in-memory cache by LRU since the store opened.
 * @property inFlight fetches running at the instant the snapshot was taken — a gauge, not a total.
 */
public data class CacheStats(
    public val hits: Long,
    public val misses: Long,
    public val evictions: Long,
    public val inFlight: Int,
) {
    /** Total reads counted: [hits] + [misses]. */
    public val reads: Long get() = hits + misses

    /** Fraction of [reads] that were [hits], in `0.0..1.0`; `0.0` when there have been no reads. */
    public val hitRate: Double get() = if (reads == 0L) 0.0 else hits.toDouble() / reads

    public companion object {
        /** All-zero counters — the opening state, and what the preview/fake stores report. */
        public val EMPTY: CacheStats = CacheStats(hits = 0, misses = 0, evictions = 0, inFlight = 0)
    }
}
