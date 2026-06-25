package io.github.quasarapps.aquifer.internal

import java.util.concurrent.atomic.AtomicLong

/**
 * A thread-safe LRU cache. Reads count as use: `get` refreshes an entry's recency, and
 * inserting beyond [maxEntries] evicts the least recently used entry.
 *
 * Every critical section is short and never suspends, so access is guarded by a plain monitor
 * rather than a coroutine `Mutex` — matching the rest of the engine's internal state
 * (`ConcurrentHashMap`, `Atomic*`) and letting reads like [keys] run without suspending.
 */
internal class MemoryCache<K : Any, V : Any>(private val maxEntries: Int) {

    init {
        require(maxEntries > 0) { "maxEntries must be positive, was $maxEntries" }
    }

    internal data class Entry<out V : Any>(
        val value: V,
        val writtenAtMillis: Long,
        /** Store-global commit sequence; orders events immune to wall-clock steps. */
        val sequence: Long,
        /** Revalidation token for conditional fetchers; see [PersistedEntry.validator]. */
        val validator: String? = null,
        /** Server-declared remaining lifetime in whole ms; see [PersistedEntry.serverFreshForMillis]. */
        val serverFreshForMillis: Long? = null,
    )

    private val lock = Any()

    // Incremented under the monitor (inside removeEldestEntry, during put), read lock-free so a
    // stats() snapshot never contends with cache traffic — consistent with hits/misses.
    private val evictionCount = AtomicLong(0)
    private val entries = object : LinkedHashMap<K, Entry<V>>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, Entry<V>>): Boolean {
            val evict = size > maxEntries
            if (evict) evictionCount.incrementAndGet()
            return evict
        }
    }

    fun get(key: K): Entry<V>? = synchronized(lock) { entries[key] }

    fun put(key: K, entry: Entry<V>) {
        synchronized(lock) { entries[key] = entry }
    }

    fun remove(key: K) {
        synchronized(lock) { entries.remove(key) }
    }

    fun clear() {
        synchronized(lock) { entries.clear() }
    }

    /** Snapshot of the resident keys; iterating the key set does not count as LRU use. */
    fun keys(): Set<K> = synchronized(lock) { LinkedHashSet(entries.keys) }

    /** Entries dropped by LRU eviction since construction; a lock-free read for stats(). */
    fun evictions(): Long = evictionCount.get()

    private companion object {
        const val INITIAL_CAPACITY = 16
        const val LOAD_FACTOR = 0.75f
    }
}
