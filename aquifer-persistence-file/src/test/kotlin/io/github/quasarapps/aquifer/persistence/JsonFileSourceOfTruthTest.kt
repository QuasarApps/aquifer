package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
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
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class User(val id: String, val name: String, val age: Int)

/** A String serializer that fails to encode one sentinel value, to force a mid-batch staging failure. */
private object FailToEncode : KSerializer<String> {
    const val POISON: String = "boom"
    override val descriptor = PrimitiveSerialDescriptor("FailToEncode", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: String) {
        check(value != POISON) { "refusing to encode the poison value" }
        encoder.encodeString(value)
    }

    override fun deserialize(decoder: Decoder): String = decoder.decodeString()
}

class JsonFileSourceOfTruthTest {

    @TempDir
    lateinit var dir: Path

    private fun store(): JsonFileSourceOfTruth<String, User> =
        JsonFileSourceOfTruth(dir.resolve("users"), User.serializer())

    @Test
    fun `written entries read back identically`() = runTest {
        val store = store()
        val entry = PersistedEntry(User("u1", "Ada Lovelace", 36), writtenAtMillis = 12345)

        store.write("u1", entry)

        assertEquals(entry, store.read("u1"))
    }

    @Test
    fun `serverFreshForMillis round-trips`() = runTest {
        val store = store()
        val entry = PersistedEntry(
            User("u1", "Ada Lovelace", 36),
            writtenAtMillis = 100,
            validator = "v",
            serverFreshForMillis = 60_000L,
        )

        store.write("u1", entry)

        assertEquals(entry, store.read("u1"))
        assertEquals(60_000L, store.read("u1")?.serverFreshForMillis)
    }

    @Test
    fun `a null serverFreshForMillis round-trips as null`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), writtenAtMillis = 100))

        assertNull(store.read("u1")?.serverFreshForMillis)
    }

    @Test
    fun `a pre-existing file without the serverFreshForMillis field decodes as null`() = runTest {
        val store = store()
        // Create the file at the right (hashed) path, then overwrite it with the old on-disk
        // format that predates server freshness: no validator/serverFreshForMillis/schemaVersion.
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), writtenAtMillis = 1))
        val file = dir.resolve("users").listDirectoryEntries().single()
        file.writeText("""{"writtenAtMillis":777,"value":{"id":"u1","name":"Ada","age":36}}""")

        val read = store.read("u1")
        assertEquals(PersistedEntry(User("u1", "Ada", 36), writtenAtMillis = 777), read)
        assertNull(read?.serverFreshForMillis)
    }

    @Test
    fun `reading an unknown key returns null`() = runTest {
        assertNull(store().read("nope"))
    }

    @Test
    fun `writing a key again replaces the previous entry`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Old", 1), writtenAtMillis = 1))
        store.write("u1", PersistedEntry(User("u1", "New", 2), writtenAtMillis = 2))

        assertEquals(PersistedEntry(User("u1", "New", 2), 2), store.read("u1"))
        assertEquals(1, dir.resolve("users").listDirectoryEntries().size)
    }

    @Test
    fun `distinct keys are stored independently`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))

        assertEquals("A", store.read("a")?.value?.name)
        assertEquals("B", store.read("b")?.value?.name)
    }

    @Test
    fun `writeAll persists every entry`() = runTest {
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
    fun `writeAll replaces an existing entry and leaves one file per key`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "Old", 1), 1))

        store.writeAll(mapOf("a" to PersistedEntry(User("a", "New", 2), 2)))

        assertEquals(PersistedEntry(User("a", "New", 2), 2), store.read("a"))
        assertEquals(1, dir.resolve("users").listDirectoryEntries().size)
    }

    @Test
    fun `writeAll on an empty map is a no-op that touches nothing`() = runTest {
        store().writeAll(emptyMap())

        assertFalse(dir.resolve("users").exists(), "an empty batch must not even create the directory")
    }

    @Test
    fun `writeAll on a bounded store evicts down to the entry cap`() = runTest {
        val store = JsonFileSourceOfTruth<String, User>(dir.resolve("users"), User.serializer(), maxEntries = 2)

        // Staged in iteration order, so "a" is the eldest and the only one over the cap of 2.
        store.writeAll(
            linkedMapOf(
                "a" to PersistedEntry(User("a", "A", 1), 1),
                "b" to PersistedEntry(User("b", "B", 2), 2),
                "c" to PersistedEntry(User("c", "C", 3), 3),
            ),
        )

        assertNull(store.read("a"), "eldest entry evicted after the batch")
        assertEquals("B", store.read("b")?.value?.name)
        assertEquals("C", store.read("c")?.value?.name)
        assertEquals(2, dir.resolve("users").listDirectoryEntries().size)
    }

    @Test
    fun `writeAll that fails to stage an entry commits nothing and leaves no temp files`() = runTest {
        val store = JsonFileSourceOfTruth<String, String>(dir.resolve("users"), FailToEncode)

        // "b" fails to encode, so stage() throws after "a" is already written to a fsynced temp.
        assertFailsWith<IllegalStateException> {
            store.writeAll(
                linkedMapOf(
                    "a" to PersistedEntry("ok", 1),
                    "b" to PersistedEntry(FailToEncode.POISON, 2),
                    "c" to PersistedEntry("ok-too", 3),
                ),
            )
        }

        val entries = dir.resolve("users").listDirectoryEntries()
        assertTrue(entries.none { it.extension == "json" }, "a mid-batch staging failure commits nothing")
        assertTrue(
            entries.none { it.fileName.toString().endsWith(".tmp") },
            "the already-staged temp for \"a\" is cleaned up, not orphaned",
        )
    }

    @Test
    fun `deleteMany removes only the listed keys`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))
        store.write("c", PersistedEntry(User("c", "C", 3), 3))

        store.deleteMany(listOf("a", "c"))

        assertNull(store.read("a"))
        assertEquals("B", store.read("b")?.value?.name)
        assertNull(store.read("c"))
    }

    @Test
    fun `deleteMany on an empty collection is a no-op`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))

        store.deleteMany(emptyList())

        assertEquals("A", store.read("a")?.value?.name)
    }

    @Test
    fun `deleteMany ignores keys with no stored entry`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))

        store.deleteMany(listOf("a", "never-written"))

        assertNull(store.read("a"))
    }

    @Test
    fun `deleteMany on a bounded store frees slots for later writes`() = runTest {
        val store = JsonFileSourceOfTruth<String, User>(dir.resolve("users"), User.serializer(), maxEntries = 3)
        store.writeAll(
            linkedMapOf(
                "a" to PersistedEntry(User("a", "A", 1), 1),
                "b" to PersistedEntry(User("b", "B", 2), 2),
                "c" to PersistedEntry(User("c", "C", 3), 3),
            ),
        ) // full at the cap of 3

        store.deleteMany(listOf("a", "b")) // drops two files and their accounting
        store.write("d", PersistedEntry(User("d", "D", 4), 4))
        store.write("e", PersistedEntry(User("e", "E", 5), 5))

        // c/d/e fit within the cap of 3: deleteMany returned the freed slots to the budget.
        assertEquals("C", store.read("c")?.value?.name)
        assertEquals("D", store.read("d")?.value?.name)
        assertEquals("E", store.read("e")?.value?.name)
        assertEquals(3, dir.resolve("users").listDirectoryEntries().size)
    }

    @Test
    fun `readAll returns every stored entry and omits unknown keys`() = runTest {
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
    fun `readAll on an empty collection is empty and touches nothing`() = runTest {
        val empty = store().readAll(emptyList())

        assertTrue(empty.isEmpty())
        assertFalse(dir.resolve("users").exists(), "an empty batch must not even create the directory")
    }

    @Test
    fun `readAll heals a corrupt entry and omits it, returning the rest`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))
        val fileA = fileOf("a")
        fileA.writeText("{ not json")

        val read = store.readAll(listOf("a", "b"))

        assertNull(read["a"], "corrupt entry is omitted, like read() returning null")
        assertEquals(User("b", "B", 2), read["b"]?.value)
        assertFalse(fileA.exists(), "the corrupt file is healed away")
    }

    @Test
    fun `readAll on a bounded store bumps recency without evicting or writing`() = runTest {
        val store = JsonFileSourceOfTruth<String, User>(dir.resolve("users"), User.serializer(), maxEntries = 2)
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2)) // recency: a (older), b (newer)

        // A batched read of "a" marks it most-recently-used, exactly as read("a") would, and must
        // not itself write or evict — readAll stays a pure read on a bounded store.
        assertEquals(User("a", "A", 1), store.readAll(listOf("a"))["a"]?.value)
        assertEquals(2, dir.resolve("users").listDirectoryEntries().size, "readAll neither writes nor evicts")

        store.write("c", PersistedEntry(User("c", "C", 3), 3)) // over the cap: evicts the now-LRU

        assertEquals("A", store.read("a")?.value?.name, "the batched read protected \"a\" from eviction")
        assertNull(store.read("b"), "\"b\" was least-recently-used after the batched read")
        assertEquals("C", store.read("c")?.value?.name)
    }

    /** SHA-256 file path for a key, mirroring the store's naming, to plant a corrupt file. */
    private fun fileOf(key: String): Path {
        val digest = java.security.MessageDigest.getInstance("SHA-256").digest(key.encodeToByteArray())
        val name = digest.joinToString("") { "%02x".format(it.toInt() and 0xff) }
        return dir.resolve("users").resolve("$name.json")
    }

    @Test
    fun `the file store opts out of key enumeration`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))

        // One-way SHA-256 filenames can't be reversed to keys, so the store keeps the null default.
        assertNull(store.keys(), "the JSON file store cannot enumerate its keys")
        assertNull(store.keysWhere { true })
    }

    @Test
    fun `filesystem-hostile keys are safe`() = runTest {
        val store = store()
        val hostile = "../../etc/passwd: CON?<>|*\u0000 \n 🦆"
        store.write(hostile, PersistedEntry(User("x", "Hostile", 1), 1))

        assertEquals("Hostile", store.read(hostile)?.value?.name)
        // Everything stays inside the dedicated directory.
        assertTrue(dir.resolve("users").listDirectoryEntries().single().fileName.toString().endsWith(".json"))
    }

    @Test
    fun `delete removes only the targeted key`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))

        store.delete("a")

        assertNull(store.read("a"))
        assertEquals("B", store.read("b")?.value?.name)
    }

    @Test
    fun `delete on a missing key is a no-op`() = runTest {
        store().delete("missing")
    }

    @Test
    fun `deleteAll clears every entry`() = runTest {
        val store = store()
        store.write("a", PersistedEntry(User("a", "A", 1), 1))
        store.write("b", PersistedEntry(User("b", "B", 2), 2))

        store.deleteAll()

        assertNull(store.read("a"))
        assertNull(store.read("b"))
    }

    @Test
    fun `a corrupt file reads as null and is healed`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 1))
        val file = dir.resolve("users").listDirectoryEntries().single()
        file.writeText("{ this is not json")

        assertNull(store.read("u1"))
        assertFalse(file.exists(), "corrupt file should be deleted")

        // The slot works again after healing.
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 2))
        assertEquals("Ada", store.read("u1")?.value?.name)
    }

    @Test
    fun `a transient io failure reads as null but keeps the file`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 1))
        val file = dir.resolve("users").listDirectoryEntries().single()

        // Swap the entry's path for a directory: reading it throws IOException, simulating a
        // transient I/O failure rather than corrupt content.
        java.nio.file.Files.delete(file)
        java.nio.file.Files.createDirectory(file)

        assertNull(store.read("u1"))
        assertTrue(file.exists(), "transient I/O failures must not delete the entry")
    }

    @Test
    fun `a file with mismatched schema reads as null and is healed`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 1))
        val file = dir.resolve("users").listDirectoryEntries().single()
        file.writeText("""{"writtenAtMillis": 1, "value": {"totally": "different"}}""")

        assertNull(store.read("u1"))
        assertFalse(file.exists())
    }

    @Test
    fun `unknown json fields are tolerated for forward compatibility`() = runTest {
        val store = store()
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 7))
        val file = dir.resolve("users").listDirectoryEntries().single()
        file.writeText(
            """{"writtenAtMillis": 7, "value": {"id": "u1", "name": "Ada", "age": 36, "addedLater": true}}""",
        )

        assertEquals(PersistedEntry(User("u1", "Ada", 36), 7), store.read("u1"))
    }

    @Test
    fun `concurrent writers to one key leave a single intact entry`() = runTest {
        val store = store()

        (1..20).map { i ->
            async(Dispatchers.IO) {
                store.write("u1", PersistedEntry(User("u1", "writer-$i", i), i.toLong()))
            }
        }.awaitAll()

        val result = store.read("u1")
        assertNotNull(result)
        // One of the written values, never an interleaving of two.
        assertEquals("writer-${result.value.age}", result.value.name)
        assertEquals(result.value.age.toLong(), result.writtenAtMillis)
        // Exactly one data file remains and no temp files leak.
        assertEquals(1, dir.resolve("users").listDirectoryEntries().size)
    }

    @Test
    fun `factory resolves the serializer from the reified type`() = runTest {
        val store = jsonFileSourceOfTruth<String, User>(dir.resolve("users"))
        store.write("u1", PersistedEntry(User("u1", "Ada", 36), 1))

        assertEquals("Ada", store.read("u1")?.value?.name)
    }
}
