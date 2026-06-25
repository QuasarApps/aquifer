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
 * - The bulk operations [writeAll]/[deleteMany] default to the single-key methods in iteration
 *   order, so they inherit those methods' contract and are not atomic unless an override makes
 *   them so. Override them when the backend can do the batch in one transaction or amortize
 *   per-call bookkeeping; the default keeps existing implementations source-compatible.
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

    /**
     * Persists every entry in [entries], replacing any previous entry for those keys — the bulk
     * equivalent of [write], used by [Aquifer.putAll]. The default writes each entry via [write]
     * in iteration order; override it when the backend can persist a batch in one transaction or
     * amortize per-write bookkeeping over the batch. Like the per-key default it is **not**
     * atomic unless an override makes it so: a failure partway through can leave an earlier prefix
     * persisted. May be invoked concurrently with any other method.
     */
    public suspend fun writeAll(entries: Map<K, PersistedEntry<V>>) {
        for ((key, entry) in entries) write(key, entry)
    }

    /**
     * Removes the entry for every key in [keys] that has one — the bulk equivalent of [delete],
     * used by [Aquifer.invalidateWhere]. The default deletes each key via [delete] in iteration
     * order; override it when the backend can delete a batch in one transaction or amortize
     * per-delete bookkeeping over the batch. Like the per-key default it is **not** atomic unless
     * an override makes it so: a failure partway through can leave an earlier prefix deleted.
     * Keys with no stored entry are ignored. May be invoked concurrently with any other method.
     */
    public suspend fun deleteMany(keys: Collection<K>) {
        for (key in keys) delete(key)
    }
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
