package io.github.quasarapps.aquifer.persistence.sqldelight

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.aquifer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.serializer
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration

@Serializable
private data class User(val id: String, val name: String, val age: Int)

/** A String serializer that refuses one sentinel value, to force a mid-transaction write failure. */
private object FailToEncode : KSerializer<String> {
    const val POISON: String = "boom"
    override val descriptor = PrimitiveSerialDescriptor("FailToEncode", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: String) {
        check(value != POISON) { "refusing to encode the poison value" }
        encoder.encodeString(value)
    }

    override fun deserialize(decoder: Decoder): String = decoder.decodeString()
}

/**
 * The SQLDelight [SqlDelightSourceOfTruth]: the [io.github.quasarapps.aquifer.SourceOfTruth]
 * contract over a JVM SQLite database, plus the batched and *enumerable* capabilities a queryable
 * backend exists to provide — including a disk-wide `invalidateWhere` driven through a real engine,
 * and safety under the concurrent, file-backed use the contract guarantees.
 */
class SqlDelightSourceOfTruthTest {

    @TempDir
    lateinit var tempDir: Path

    /** A fresh in-memory store; the driver is kept alive by the adapter for the test's lifetime. */
    private fun store(): SqlDelightSourceOfTruth<String, User> {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        return SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })
    }

    @Test
    fun `written entries read back identically`() = runTest {
        val store = store()
        val entry = PersistedEntry(User("u1", "Ada", 36), writtenAtMillis = 12345)

        store.write("u1", entry)

        assertEquals(entry, store.read("u1"))
    }

    @Test
    fun `validator and serverFreshForMillis round-trip`() = runTest {
        val store = store()
        val entry = PersistedEntry(
            User("u1", "Ada", 36),
            writtenAtMillis = 100,
            validator = "etag",
            serverFreshForMillis = 60_000L,
        )

        store.write("u1", entry)

        assertEquals(entry, store.read("u1"))
    }

    @Test
    fun `reading an unknown key returns null`() = runTest {
        assertNull(store().read("nope"))
    }

    @Test
    fun `writing a key again replaces the previous entry`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Old", 1), 1))
        store.write("u1", PersistedEntry(User("u1", "New", 2), 2))

        assertEquals(PersistedEntry(User("u1", "New", 2), 2), store.read("u1"))
    }

    @Test
    fun `delete removes only the targeted key, deleteAll clears everything`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))

        store.delete("a")
        assertNull(store.read("a"))
        assertEquals("B", store.read("b")?.value?.name)

        store.deleteAll()
        assertNull(store.read("b"))
    }

    @Test
    fun `readAll returns stored entries and omits unknown keys`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))

        val read = store.readAll(listOf("a", "b", "missing"))

        assertEquals(
            mapOf(
                "a" to PersistedEntry(User("a", "A", 1), 1),
                "b" to PersistedEntry(User("b", "B", 2), 2),
            ),
            read,
        )
    }

    @Test
    fun `readAll on an empty collection is empty`() = runTest {
        assertTrue(store().readAll(emptyList()).isEmpty())
    }

    @Test
    fun `readAll omits an undecodable row and returns the decodable siblings`() = runTest {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        // "a" holds an Int; reading it back as a User can't decode, so the batch omits it but keeps "b".
        SqlDelightSourceOfTruth(driver, serializer<Int>(), { it }, { it }).write("a", PersistedEntry(42, 0))
        val asUser = SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })
        asUser.write("b", PersistedEntry(User("b", "B", 2), 0))

        val read = asUser.readAll(listOf("a", "b"))

        assertEquals(setOf("b"), read.keys)
        assertEquals(User("b", "B", 2), read["b"]?.value)
    }

    @Test
    fun `writeAll persists every entry in one transaction`() = runTest {
        val store = store()
        store.writeAll(
            mapOf(
                "a" to PersistedEntry(User("a", "A", 1), 1),
                "b" to PersistedEntry(User("b", "B", 2), 2),
            ),
        )

        assertEquals("A", store.read("a")?.value?.name)
        assertEquals("B", store.read("b")?.value?.name)
    }

    @Test
    fun `writeAll rolls back entirely when an entry fails to encode`() = runTest {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        val store = SqlDelightSourceOfTruth(driver, FailToEncode, { it }, { it })

        // "b" throws mid-transaction, after "a" was already staged: the transaction must roll back.
        assertFailsWith<IllegalStateException> {
            store.writeAll(
                linkedMapOf(
                    "a" to PersistedEntry("ok", 1),
                    "b" to PersistedEntry(FailToEncode.POISON, 2),
                    "c" to PersistedEntry("ok-too", 3),
                ),
            )
        }

        assertNull(store.read("a"), "the whole batch rolled back, including the entry staged before the failure")
    }

    @Test
    fun `deleteMany removes only the listed keys and ignores unknown ones`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))
        store.write("c", PersistedEntry(User("c", "C", 3), 3))

        store.deleteMany(listOf("a", "c", "never-written"))

        assertNull(store.read("a"))
        assertEquals("B", store.read("b")?.value?.name)
        assertNull(store.read("c"))
    }

    @Test
    fun `bulk readAll and deleteMany chunk batches larger than one statement`() = runTest {
        val store = store()
        // Far more keys than MAX_KEYS_PER_STATEMENT (900), so the IN-clause must be chunked.
        val many = (1..2000).associate { "k$it" to PersistedEntry(User("k$it", "N", it), it.toLong()) }
        store.writeAll(many)

        assertEquals(2000, store.readAll(many.keys).size) // chunked reads union back to the full set

        store.deleteMany(many.keys) // chunked deletes, all in one transaction
        assertTrue(store.readAll(many.keys).isEmpty())
    }

    @Test
    fun `keys enumerates every stored key and keysWhere filters`() = runTest {
        val store = store()
        store.write("tenant:a", PersistedEntry(User("a", "A", 1), 1))
        store.write("tenant:b", PersistedEntry(User("b", "B", 2), 2))
        store.write("other:c", PersistedEntry(User("c", "C", 3), 3))

        assertEquals(setOf("tenant:a", "tenant:b", "other:c"), store.keys())
        assertEquals(setOf("tenant:a", "tenant:b"), store.keysWhere { it.startsWith("tenant:") })
    }

    @Test
    fun `non-string keys round-trip and enumerate through a codec`() = runTest {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        val store = SqlDelightSourceOfTruth<Int, User>(driver, User.serializer(), { it.toString() }, { it.toInt() })

        store.write(7, PersistedEntry(User("7", "Seven", 7), 0))

        assertEquals(User("7", "Seven", 7), store.read(7)?.value)
        assertEquals(setOf(7), store.keys())
    }

    @Test
    fun `a row whose value no longer decodes reads as null`() = runTest {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        SqlDelightSourceOfTruth(driver, serializer<Int>(), { it }, { it })
            .write("k", PersistedEntry(42, writtenAtMillis = 0))
        val asUser = SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })

        assertNull(asUser.read("k"))
    }

    @Test
    fun `getAll serves seeded rows from the SQLDelight store`() = runTest {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SqlDelightSourceOfTruth.Schema.create(driver)
        val disk = SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })
        disk.writeAll(
            mapOf(
                "a" to PersistedEntry(User("a", "A", 1), 0),
                "b" to PersistedEntry(User("b", "B", 2), 0),
            ),
        )
        val store = aquifer<String, User> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("served from disk") }
            freshness { timeToLive = Duration.INFINITE }
        }

        assertEquals(
            mapOf("a" to User("a", "A", 1), "b" to User("b", "B", 2)),
            store.getAll(setOf("a", "b"), Freshness.CacheFirst),
        )
    }

    @Test
    fun `disk-wide invalidateWhere reaches a never-loaded key on a file-backed store`() = runTest {
        val driver = JdbcSqliteDriver("jdbc:sqlite:${tempDir.resolve("cache.db")}")
        SqlDelightSourceOfTruth.Schema.create(driver)
        val disk = SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })
        // Seed straight into the store: the engine has no in-process trace of "cold".
        disk.write("cold", PersistedEntry(User("cold", "Cold", 1), writtenAtMillis = 0))
        val store = aquifer<String, User> {
            scope(backgroundScope)
            persistence(disk)
            fetcher { error("never fetched") }
        }

        store.invalidateWhere { it == "cold" }

        // The payoff: SQLDelight's enumerability backs a disk-wide invalidateWhere, end-to-end on disk.
        assertNull(disk.read("cold"))
        assertFailsWith<CacheMissException> { store.get("cold", Freshness.CacheOnly) }
        driver.close()
    }

    @Test
    fun `concurrent writers to a file-backed store all persist without lock contention`() = runTest {
        val driver = JdbcSqliteDriver("jdbc:sqlite:${tempDir.resolve("cache.db")}")
        SqlDelightSourceOfTruth.Schema.create(driver)
        val store = SqlDelightSourceOfTruth(driver, User.serializer(), { it }, { it })

        // A file-backed driver opens a connection per thread; without the store's serialization this
        // contends on the SQLite file lock and throws SQLITE_BUSY (losing writes). It must not.
        (1..50).map { i ->
            async(Dispatchers.IO) {
                repeat(3) { j -> store.write("k$i-$j", PersistedEntry(User("k$i-$j", "N", i), i.toLong())) }
            }
        }.awaitAll()

        assertEquals(150, store.keys().size)
        driver.close()
    }

    @Test
    fun `a file-backed store survives a driver reopen`() = runTest {
        val url = "jdbc:sqlite:${tempDir.resolve("cache.db")}"
        val first = JdbcSqliteDriver(url)
        SqlDelightSourceOfTruth.Schema.create(first)
        SqlDelightSourceOfTruth(first, User.serializer(), { it }, { it })
            .write("u", PersistedEntry(User("u", "U", 1), 1))
        first.close()

        // Reopen the same file with a fresh driver (no schema create): the row must still be there.
        val second = JdbcSqliteDriver(url)
        val reopened = SqlDelightSourceOfTruth(second, User.serializer(), { it }, { it })

        assertEquals(User("u", "U", 1), reopened.read("u")?.value)
        second.close()
    }
}
