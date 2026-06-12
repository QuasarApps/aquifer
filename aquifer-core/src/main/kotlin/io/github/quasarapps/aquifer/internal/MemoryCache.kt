package io.github.quasarapps.aquifer.internal

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A coroutine-safe LRU cache. Reads count as use: `get` refreshes an entry's recency, and
 * inserting beyond [maxEntries] evicts the least recently used entry.
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
    )

    private val mutex = Mutex()
    private val entries = object : LinkedHashMap<K, Entry<V>>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, Entry<V>>): Boolean =
            size > maxEntries
    }

    suspend fun get(key: K): Entry<V>? = mutex.withLock { entries[key] }

    suspend fun put(key: K, entry: Entry<V>): Unit = mutex.withLock { entries[key] = entry }

    suspend fun remove(key: K): Unit = mutex.withLock<Unit> { entries.remove(key) }

    suspend fun clear(): Unit = mutex.withLock { entries.clear() }

    private companion object {
        const val INITIAL_CAPACITY = 16
        const val LOAD_FACTOR = 0.75f
    }
}
