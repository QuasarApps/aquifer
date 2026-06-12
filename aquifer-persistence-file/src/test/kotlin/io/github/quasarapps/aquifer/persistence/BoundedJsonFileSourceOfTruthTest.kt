package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.security.MessageDigest
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class Payload(val data: String)

/** LRU bounding of [JsonFileSourceOfTruth]: `maxEntries`/`maxBytes`, seeding, and temp GC. */
class BoundedJsonFileSourceOfTruthTest {

    @TempDir
    lateinit var dir: Path

    private val storeDir: Path get() = dir.resolve("payloads")

    private fun store(maxEntries: Int? = null, maxBytes: Long? = null): JsonFileSourceOfTruth<String, Payload> =
        JsonFileSourceOfTruth(storeDir, Payload.serializer(), maxEntries = maxEntries, maxBytes = maxBytes)

    private suspend fun JsonFileSourceOfTruth<String, Payload>.put(key: String, data: String = key) {
        write(key, PersistedEntry(Payload(data), writtenAtMillis = 1))
    }

    private fun fileOf(key: String): Path {
        val digest = MessageDigest.getInstance("SHA-256").digest(key.encodeToByteArray())
        val name = digest.joinToString("") { "%02x".format(it.toInt() and 0xff) }
        return storeDir.resolve("$name.json")
    }

    private fun jsonFileCount(): Int = storeDir.listDirectoryEntries().count { it.extension == "json" }

    @Test
    fun `maxEntries evicts the least recently written entry`() = runTest {
        val store = store(maxEntries = 2)
        store.put("a")
        store.put("b")
        store.put("c")

        assertNull(store.read("a"))
        assertEquals("b", store.read("b")?.value?.data)
        assertEquals("c", store.read("c")?.value?.data)
        assertEquals(2, jsonFileCount())
    }

    @Test
    fun `reading an entry protects it from eviction`() = runTest {
        val store = store(maxEntries = 2)
        store.put("a")
        store.put("b")
        store.read("a") // now more recently used than b

        store.put("c")

        assertEquals("a", store.read("a")?.value?.data)
        assertNull(store.read("b"))
    }

    @Test
    fun `rewriting a key refreshes its recency without evicting others`() = runTest {
        val store = store(maxEntries = 2)
        store.put("a")
        store.put("b")
        store.put("a", data = "a2") // rewrite: still 2 entries, a now most recent

        assertEquals(2, jsonFileCount())

        store.put("c")

        assertEquals("a2", store.read("a")?.value?.data)
        assertNull(store.read("b"))
    }

    @Test
    fun `maxBytes evicts eldest entries until the total fits`() = runTest {
        // Each file is its ~1000-char payload plus a fixed envelope well under 100 bytes:
        // two entries fit a 2200-byte budget, three cannot.
        val store = store(maxBytes = 2200)
        store.put("a", data = "x".repeat(1000))
        store.put("b", data = "y".repeat(1000))
        store.put("c", data = "z".repeat(1000))

        assertNull(store.read("a"))
        assertEquals(1000, store.read("b")?.value?.data?.length)
        assertEquals(1000, store.read("c")?.value?.data?.length)
    }

    @Test
    fun `an entry larger than maxBytes alone never persists`() = runTest {
        val store = store(maxBytes = 500)
        store.put("huge", data = "x".repeat(1000))

        assertNull(store.read("huge"))
        assertEquals(0, jsonFileCount())
    }

    @Test
    fun `both caps are enforced together`() = runTest {
        // Entry budget allows three, byte budget only two of these.
        val store = store(maxEntries = 3, maxBytes = 2200)
        store.put("a", data = "x".repeat(1000))
        store.put("b", data = "y".repeat(1000))
        store.put("c", data = "z".repeat(1000))

        assertNull(store.read("a"))
        assertEquals(2, jsonFileCount())
    }

    @Test
    fun `delete frees budget`() = runTest {
        val store = store(maxEntries = 2)
        store.put("a")
        store.put("b")
        store.delete("a")
        store.put("c")

        assertEquals("b", store.read("b")?.value?.data)
        assertEquals("c", store.read("c")?.value?.data)
    }

    @Test
    fun `deleteAll resets the accounting`() = runTest {
        val store = store(maxEntries = 2)
        store.put("a")
        store.put("b")
        store.deleteAll()

        store.put("c")
        store.put("d")

        assertEquals("c", store.read("c")?.value?.data)
        assertEquals("d", store.read("d")?.value?.data)
        assertEquals(2, jsonFileCount())
    }

    @Test
    fun `a healed corrupt file frees its budget share`() = runTest {
        val store = store(maxBytes = 2200)
        store.put("a", data = "x".repeat(1000))
        store.put("b", data = "y".repeat(1000))
        fileOf("a").writeText("{ not json")

        assertNull(store.read("a")) // healed: deleted and forgotten

        // Its budget share is genuinely free again: a third large entry fits with b.
        store.put("c", data = "z".repeat(1000))
        assertEquals(1000, store.read("b")?.value?.data?.length)
        assertEquals(1000, store.read("c")?.value?.data?.length)
    }

    @Test
    fun `a new instance seeds recency from file timestamps and trims to budget`() = runTest {
        val writer = store() // unbounded: nothing evicted while populating
        writer.put("a")
        writer.put("b")
        writer.put("c")
        Files.setLastModifiedTime(fileOf("a"), FileTime.fromMillis(1_000))
        Files.setLastModifiedTime(fileOf("b"), FileTime.fromMillis(3_000))
        Files.setLastModifiedTime(fileOf("c"), FileTime.fromMillis(2_000))

        // Limits "lowered between releases": the reopened store trims eldest-first on first use.
        val reopened = store(maxEntries = 2)

        assertNull(reopened.read("a"))
        assertEquals("b", reopened.read("b")?.value?.data)
        assertEquals("c", reopened.read("c")?.value?.data)
    }

    @Test
    fun `orphaned temp files are cleaned on first use, bounded or not`() = runTest {
        store().put("real")
        val orphan = storeDir.resolve("deadbeef.json.123e4567.tmp")
        orphan.writeText("torn write left behind by a crash")

        val unbounded = store()
        assertEquals("real", unbounded.read("real")?.value?.data)
        assertFalse(orphan.exists(), "first filesystem touch should GC orphaned temps")

        orphan.writeText("again")
        val bounded = store(maxEntries = 5)
        assertEquals("real", bounded.read("real")?.value?.data)
        assertFalse(orphan.exists())
    }

    @Test
    fun `temp GC tolerates a store directory that does not exist yet`() = runTest {
        val store = store(maxEntries = 2)
        assertNull(store.read("anything")) // directory absent: housekeeping is a quiet no-op

        store.put("a")
        assertEquals("a", store.read("a")?.value?.data)
    }

    @Test
    fun `concurrent writers never leave the store over budget`() = runTest {
        val store = store(maxEntries = 5)

        (1..20).map { i ->
            async(Dispatchers.IO) { store.put("key-$i") }
        }.awaitAll()

        assertEquals(5, jsonFileCount())
        assertTrue(storeDir.listDirectoryEntries().none { it.fileName.toString().endsWith(".tmp") })
    }

    @Test
    fun `unbounded stores never evict`() = runTest {
        val store = store()
        repeat(50) { i -> store.put("key-$i") }

        assertEquals(50, jsonFileCount())
    }

    @Test
    fun `limits must be positive`() {
        assertFailsWith<IllegalArgumentException> { store(maxEntries = 0) }
        assertFailsWith<IllegalArgumentException> { store(maxEntries = -1) }
        assertFailsWith<IllegalArgumentException> { store(maxBytes = 0L) }
        assertFailsWith<IllegalArgumentException> { store(maxBytes = -10L) }
    }

    @Test
    fun `factory passes the limits through`() = runTest {
        storeDir.createDirectories()
        val store = jsonFileSourceOfTruth<String, Payload>(storeDir, maxEntries = 1)
        store.write("a", PersistedEntry(Payload("a"), 1))
        store.write("b", PersistedEntry(Payload("b"), 2))

        assertNull(store.read("a"))
        assertEquals("b", store.read("b")?.value?.data)
    }
}
