package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class PersonV1(val id: String, val name: String)

@Serializable
private data class PersonV2(val id: String, val firstName: String, val lastName: String)

class SchemaMigrationTest {

    @TempDir
    lateinit var dir: Path

    // v0/v1 stored a single "name"; v2 splits it into firstName/lastName.
    private val splitName: (Int, JsonElement) -> JsonElement? = { fromVersion, value ->
        when (fromVersion) {
            0, 1 -> buildJsonObject {
                val obj = value.jsonObject
                put("id", obj.getValue("id"))
                val parts = obj.getValue("name").jsonPrimitive.content.split(" ", limit = 2)
                put("firstName", parts.first())
                put("lastName", parts.getOrElse(1) { "" })
            }
            else -> null
        }
    }

    // A store at schemaVersion 0 — the legacy on-disk format, no version field written.
    private fun legacyStore() =
        JsonFileSourceOfTruth<String, PersonV1>(dir.resolve("p"), PersonV1.serializer())

    private fun v1Store() =
        JsonFileSourceOfTruth<String, PersonV1>(dir.resolve("p"), PersonV1.serializer(), schemaVersion = 1)

    private fun v2Store(migrate: (Int, JsonElement) -> JsonElement? = splitName) =
        JsonFileSourceOfTruth<String, PersonV2>(
            dir.resolve("p"),
            PersonV2.serializer(),
            schemaVersion = 2,
            migrate = migrate,
        )

    private fun jsonFiles() = dir.resolve("p").listDirectoryEntries().filter { it.extension == "json" }

    @Test
    fun `migrate upgrades an older entry to the current shape`() = runTest {
        v1Store().write("u1", PersistedEntry(PersonV1("u1", "Ada Lovelace"), writtenAtMillis = 10))

        val read = v2Store().read("u1")

        assertEquals(PersonV2("u1", "Ada", "Lovelace"), read?.value)
        assertEquals(10, read?.writtenAtMillis) // the write timestamp survives migration
    }

    @Test
    fun `a legacy version-0 entry migrates, and version-0 writes no version field`() = runTest {
        legacyStore().write("u1", PersistedEntry(PersonV1("u1", "Ada Lovelace"), writtenAtMillis = 10))

        val text = jsonFiles().single().readText()
        assertFalse(text.contains("schemaVersion"), "version-0 store must write the legacy format: $text")
        assertEquals(PersonV1("u1", "Ada Lovelace"), legacyStore().read("u1")?.value)

        // The same legacy file is reachable by a v2 store, which migrates fromVersion 0.
        assertEquals(PersonV2("u1", "Ada", "Lovelace"), v2Store().read("u1")?.value)
    }

    @Test
    fun `migrate is not called when the stored version is already current`() = runTest {
        v2Store().write("u1", PersistedEntry(PersonV2("u1", "Ada", "Lovelace"), writtenAtMillis = 10))

        val boom: (Int, JsonElement) -> JsonElement? = { _, _ -> error("migrate must not run for a current entry") }
        assertEquals(PersonV2("u1", "Ada", "Lovelace"), v2Store(boom).read("u1")?.value)
    }

    @Test
    fun `migrate returning null drops the entry and heals the slot`() = runTest {
        v1Store().write("u1", PersistedEntry(PersonV1("u1", "Ada Lovelace"), writtenAtMillis = 10))

        val dropAll: (Int, JsonElement) -> JsonElement? = { _, _ -> null }
        assertNull(v2Store(dropAll).read("u1"))
        assertTrue(jsonFiles().isEmpty(), "a dropped entry should be healed away")
    }

    @Test
    fun `an entry stored above the current version is dropped`() = runTest {
        v2Store().write("u1", PersistedEntry(PersonV2("u1", "Ada", "Lovelace"), writtenAtMillis = 10))

        // A store one schema behind can't know the newer shape, so it drops and refetches.
        val behind = JsonFileSourceOfTruth<String, PersonV2>(dir.resolve("p"), PersonV2.serializer(), schemaVersion = 1)
        assertNull(behind.read("u1"))
        assertTrue(jsonFiles().isEmpty(), "a future-versioned entry should be healed away")
    }

    @Test
    fun `a version-0 store drops an entry stamped at a higher version`() = runTest {
        // Exercises the configured-version-0 fast path: a future build wrote at v2, but a v0
        // store can't know that shape, so it drops the entry and heals the slot rather than
        // serving a future entry as if it were v0 — even when the value still binds to its type.
        v2Store().write("u1", PersistedEntry(PersonV2("u1", "Ada", "Lovelace"), writtenAtMillis = 10))

        val legacy = JsonFileSourceOfTruth<String, PersonV2>(dir.resolve("p"), PersonV2.serializer())
        assertNull(legacy.read("u1"))
        assertTrue(jsonFiles().isEmpty(), "a future-versioned entry should be healed away by a v0 store")
    }

    @Test
    fun `a migration producing an undecodable tree heals the entry`() = runTest {
        v1Store().write("u1", PersistedEntry(PersonV1("u1", "Ada Lovelace"), writtenAtMillis = 10))

        // Migrated JSON missing the required firstName/lastName fails to decode into PersonV2.
        val garbage: (Int, JsonElement) -> JsonElement? = { _, _ -> buildJsonObject { put("nope", "x") } }
        assertNull(v2Store(garbage).read("u1"))
        assertTrue(jsonFiles().isEmpty(), "an undecodable migration result should be healed away")
    }

    @Test
    fun `a write after migration restamps the entry at the current version`() = runTest {
        v1Store().write("u1", PersistedEntry(PersonV1("u1", "Ada Lovelace"), writtenAtMillis = 10))
        val v2 = v2Store()
        assertEquals(PersonV2("u1", "Ada", "Lovelace"), v2.read("u1")?.value) // migrated on read

        v2.write("u1", PersistedEntry(PersonV2("u1", "Grace", "Hopper"), writtenAtMillis = 20))

        // Now stored at v2, so a store whose migrate would throw reads it without migrating.
        val boom: (Int, JsonElement) -> JsonElement? = { _, _ -> error("migrate must not run after restamp") }
        assertEquals(PersonV2("u1", "Grace", "Hopper"), v2Store(boom).read("u1")?.value)
    }

    @Test
    fun `negative schemaVersion is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            JsonFileSourceOfTruth<String, PersonV2>(dir.resolve("p"), PersonV2.serializer(), schemaVersion = -1)
        }
    }
}
