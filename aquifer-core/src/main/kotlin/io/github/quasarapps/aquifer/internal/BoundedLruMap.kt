package io.github.quasarapps.aquifer.internal

/**
 * A thread-safe, size-bounded LRU map. Access-ordered: [get] and [put] refresh an entry's
 * recency, and a [put] beyond [maxEntries] evicts the least-recently-used entry inline.
 *
 * Every critical section is short and never suspends, so access is guarded by a plain monitor
 * rather than a coroutine `Mutex` — matching [MemoryCache] and the rest of the engine's internal
 * state. Guarding reads too (unlike a lock-free `ConcurrentHashMap`) is what makes the
 * access-order structural mutation on [get] safe; the monitor is independent of the engine's
 * `commitGuard`, and its sections never acquire it, so there is no lock-ordering hazard.
 */
internal class BoundedLruMap<K : Any, V : Any>(private val maxEntries: Int) {

    init {
        require(maxEntries > 0) { "maxEntries must be positive, was $maxEntries" }
    }

    private val lock = Any()
    private val entries = object : LinkedHashMap<K, V>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>): Boolean = size > maxEntries
    }

    fun get(key: K): V? = synchronized(lock) { entries[key] }

    fun put(key: K, value: V) {
        synchronized(lock) { entries[key] = value }
    }

    fun remove(key: K) {
        synchronized(lock) { entries.remove(key) }
    }

    fun clear() {
        synchronized(lock) { entries.clear() }
    }

    /** Snapshot of the current keys; iterating them does not count as LRU use. */
    fun keys(): Set<K> = synchronized(lock) { LinkedHashSet(entries.keys) }

    private companion object {
        const val INITIAL_CAPACITY = 16
        const val LOAD_FACTOR = 0.75f
    }
}
