package io.github.quasarapps.aquifer

/**
 * Pluggable persistent storage behind an [Aquifer] — typically disk — so data survives
 * process death and cold starts render instantly from local storage.
 *
 * When configured via [AquiferBuilder.persistence], the store reads through it on memory
 * misses (hydrating the in-memory cache, including after LRU eviction) and writes through to
 * it on successful fetches and local [Aquifer.put]s. Values carry their original write
 * timestamp, so time-to-live decisions remain correct across process restarts.
 *
 * ### Contract
 *
 * - Methods may be invoked concurrently from arbitrary threads; implementations must be safe
 *   under concurrent use. Aquifer does not lock around persistence calls.
 * - [read] returns `null` for unknown keys. Implementations should also return `null` (rather
 *   than throw) for entries they can no longer decode, treating them as absent.
 * - Failures thrown by [write]/[delete]/[deleteAll] propagate to direct callers
 *   ([Aquifer.put], [Aquifer.invalidate], [Aquifer.invalidateAll]); the background
 *   write-through after a successful fetch is best-effort, and a failure there is swallowed
 *   so a fetched value is never lost to a storage error.
 *
 * See `aquifer-persistence-file` for a ready-made JSON-files implementation.
 */
public interface SourceOfTruth<K : Any, V : Any> {

    /** Returns the persisted entry for [key], or `null` when none exists (or it is unreadable). */
    public suspend fun read(key: K): PersistedEntry<V>?

    /** Persists [entry] for [key], replacing any previous entry. */
    public suspend fun write(key: K, entry: PersistedEntry<V>)

    /** Removes the entry for [key], if any. */
    public suspend fun delete(key: K)

    /** Removes all entries written by this source of truth. */
    public suspend fun deleteAll()
}

/**
 * A value plus the wall-clock time it was originally written, as stored in a [SourceOfTruth].
 * The timestamp is what keeps staleness decisions correct across process restarts.
 */
public data class PersistedEntry<V : Any>(
    val value: V,
    val writtenAtMillis: Long,
    /**
     * Opaque revalidation token (e.g. an HTTP `ETag`) carried for
     * [conditional fetchers][AquiferBuilder.conditionalFetcher]; `null` when none. Stores
     * that persist it keep conditional fetching cheap across process restarts.
     */
    val validator: String? = null,
    /**
     * Server-declared remaining lifetime at [writtenAtMillis], in whole milliseconds (a `Long`
     * for on-disk format stability, like [writtenAtMillis]); `null` when the origin expressed
     * no opinion. Derived from [FetchResult.Fresh.freshFor]; a store that persists it makes the
     * server's freshness horizon authoritative across process restarts (see that field).
     */
    val serverFreshForMillis: Long? = null,
)
