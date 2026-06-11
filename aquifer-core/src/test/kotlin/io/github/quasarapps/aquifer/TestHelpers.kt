package io.github.quasarapps.aquifer

import kotlinx.coroutines.yield

/** Minimal in-memory [SourceOfTruth] for tests that assert on persisted contents. */
class InMemorySourceOfTruth<K : Any, V : Any> : SourceOfTruth<K, V> {

    val storage = mutableMapOf<K, PersistedEntry<V>>()

    override suspend fun read(key: K): PersistedEntry<V>? = storage[key]

    override suspend fun write(key: K, entry: PersistedEntry<V>) {
        storage[key] = entry
    }

    override suspend fun delete(key: K) {
        storage.remove(key)
    }

    override suspend fun deleteAll() {
        storage.clear()
    }
}

/**
 * Suspends the test coroutine long enough for work already scheduled on the store's
 * (background) scope to run to completion.
 *
 * `runTest` only executes background work while the test coroutine itself is suspended, so
 * fire-and-forget effects — like a stale-while-revalidate refresh — need an explicit
 * suspension point before they can be asserted on.
 */
suspend fun settle() {
    repeat(8) { yield() }
}
