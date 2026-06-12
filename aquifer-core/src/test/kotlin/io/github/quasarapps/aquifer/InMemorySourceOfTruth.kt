package io.github.quasarapps.aquifer

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
