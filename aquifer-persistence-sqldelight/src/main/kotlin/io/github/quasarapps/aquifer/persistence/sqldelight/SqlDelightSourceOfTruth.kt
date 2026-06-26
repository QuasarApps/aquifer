package io.github.quasarapps.aquifer.persistence.sqldelight

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlSchema
import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.SourceOfTruth
import io.github.quasarapps.aquifer.persistence.sqldelight.db.AquiferDatabase
import io.github.quasarapps.aquifer.persistence.sqldelight.db.Entry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.coroutines.CoroutineContext

/**
 * A [SourceOfTruth] backed by a SQLDelight database — a single table of `(key, value, timestamp,
 * validator, serverFreshForMillis)` rows — so the cache survives process death and a queryable
 * backend can express the batched and disk-wide operations the file store cannot.
 *
 * ```
 * val driver = JdbcSqliteDriver("jdbc:sqlite:cache.db")
 * SqlDelightSourceOfTruth.Schema.create(driver) // once, on a fresh database
 * val users = aquifer<String, User> {
 *     fetcher { api.fetchUser(it) }
 *     persistence(SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it }))
 * }
 * ```
 *
 * ### Keys and values
 *
 * - The value is stored as JSON via [valueSerializer]. An entry whose stored JSON no longer
 *   decodes is treated as absent ([read] returns `null`), per the [SourceOfTruth] contract;
 *   unknown JSON fields are ignored, so adding a field to your model keeps old rows readable.
 * - The key is stored as text via [keyEncode]; [keyDecode] must invert it, because [keys]
 *   reconstructs the original keys from storage. [keyEncode] must be **injective** — two distinct
 *   keys must not encode to the same text, or one would shadow the other. For `String` keys both
 *   are the identity.
 *
 * ### Batching and enumeration
 *
 * - [readAll]/[deleteMany] run as a single `IN`-clause statement (chunked under SQLite's
 *   bound-variable cap only for batches larger than [MAX_KEYS_PER_STATEMENT]) and [writeAll] as one
 *   transaction, so the engine's batch paths ([Aquifer.getAll]/[Aquifer.putAll]/
 *   [Aquifer.invalidateWhere]) do one round-trip instead of N.
 * - [keys]/[keysWhere] are supported (never `null`), so [Aquifer.invalidateWhere] is **disk-wide**:
 *   its predicate reaches every persisted key, not just those tracked in memory.
 *
 * ### Threading & schema
 *
 * - The [SourceOfTruth] contract allows concurrent calls; this store **serializes** every operation
 *   onto one connection at a time (running it on [ioContext], default [Dispatchers.IO]), so it is
 *   safe whatever [SqlDriver] you supply — a file-backed `JdbcSqliteDriver` otherwise opens a
 *   connection per thread with no busy handling and would trip `SQLITE_BUSY` under concurrency. Use
 *   one store instance per database file (it is that file's only writer).
 * - The caller owns the schema lifecycle: create it once with [Schema] (e.g.
 *   `SqlDelightSourceOfTruth.Schema.create(driver)`), or hand [Schema] to a driver that manages
 *   versioning/migration. The store itself never creates or migrates the schema.
 */
@Suppress("TooManyFunctions")
public class SqlDelightSourceOfTruth<K : Any, V : Any>(
    driver: SqlDriver,
    private val valueSerializer: KSerializer<V>,
    private val keyEncode: (K) -> String,
    private val keyDecode: (String) -> K,
    private val ioContext: CoroutineContext = Dispatchers.IO,
) : SourceOfTruth<K, V> {

    private val queries = AquiferDatabase(driver).entryQueries
    private val json = Json { ignoreUnknownKeys = true }

    // Serializes all access so a single connection is never touched concurrently; the file-backed
    // JdbcSqliteDriver is one connection per thread with no busy_timeout, so concurrent access would
    // otherwise trip SQLITE_BUSY (the JsonFileSourceOfTruth store serializes the same way).
    private val mutex = Mutex()

    private suspend fun <T> onDatabase(block: () -> T): T = mutex.withLock { withContext(ioContext) { block() } }

    private fun encode(value: V): String = json.encodeToString(valueSerializer, value)

    private fun upsertEntry(key: K, entry: PersistedEntry<V>) =
        queries.upsert(
            keyEncode(key),
            encode(entry.value),
            entry.writtenAtMillis,
            entry.validator,
            entry.serverFreshForMillis,
        )

    override suspend fun read(key: K): PersistedEntry<V>? = onDatabase {
        queries.selectByKey(keyEncode(key)).executeAsOneOrNull()?.decode()
    }

    override suspend fun write(key: K, entry: PersistedEntry<V>): Unit = onDatabase {
        upsertEntry(key, entry)
    }

    override suspend fun delete(key: K): Unit = onDatabase {
        queries.deleteByKey(keyEncode(key))
    }

    override suspend fun deleteAll(): Unit = onDatabase {
        queries.deleteAll()
    }

    override suspend fun readAll(keys: Collection<K>): Map<K, PersistedEntry<V>> = onDatabase {
        if (keys.isEmpty()) return@onDatabase emptyMap()
        // Encoded text -> original key, so rows can be mapped back; chunked to stay under the cap.
        val byEncoded = LinkedHashMap<String, K>(keys.size)
        for (key in keys) byEncoded[keyEncode(key)] = key
        val result = LinkedHashMap<K, PersistedEntry<V>>(byEncoded.size)
        for (chunk in byEncoded.keys.chunked(MAX_KEYS_PER_STATEMENT)) {
            for (row in queries.selectByKeys(chunk).executeAsList()) {
                val key = byEncoded[row.key] ?: continue
                row.decode()?.let { result[key] = it }
            }
        }
        result
    }

    override suspend fun writeAll(entries: Map<K, PersistedEntry<V>>): Unit = onDatabase {
        if (entries.isEmpty()) return@onDatabase
        queries.transaction {
            for ((key, entry) in entries) upsertEntry(key, entry)
        }
    }

    override suspend fun deleteMany(keys: Collection<K>): Unit = onDatabase {
        if (keys.isEmpty()) return@onDatabase
        // One transaction over the chunks keeps the batch atomic, which the engine relies on.
        val encoded = keys.map(keyEncode)
        queries.transaction {
            for (chunk in encoded.chunked(MAX_KEYS_PER_STATEMENT)) queries.deleteByKeys(chunk)
        }
    }

    override suspend fun keys(): Set<K> = onDatabase {
        queries.selectAllKeys().executeAsList().mapTo(LinkedHashSet(), keyDecode)
    }

    private fun Entry.decode(): PersistedEntry<V>? =
        try {
            PersistedEntry(
                value = json.decodeFromString(valueSerializer, value_),
                writtenAtMillis = writtenAtMillis,
                validator = validator,
                serverFreshForMillis = serverFreshForMillis,
            )
        } catch (_: IllegalArgumentException) {
            // kotlinx SerializationException is an IllegalArgumentException: an entry that no longer
            // decodes (or whose serializer rejects it) is treated as absent, like read() returns null.
            null
        }

    /** Schema and the chunking bound for the backing table; see the class docs for the lifecycle. */
    public companion object {
        // SQLite caps host parameters per statement (999 before 3.32, 32766 after); 900 is safe on
        // every version, and a cache batch is almost always far smaller, so it stays one round-trip.
        private const val MAX_KEYS_PER_STATEMENT = 900

        /**
         * The database schema to create (or migrate) on the [SqlDriver] before the store is used —
         * e.g. `SqlDelightSourceOfTruth.Schema.create(driver)`. Exposed here so callers never need
         * to reference the generated database package.
         */
        public val Schema: SqlSchema<QueryResult.Value<Unit>>
            get() = AquiferDatabase.Schema
    }
}
