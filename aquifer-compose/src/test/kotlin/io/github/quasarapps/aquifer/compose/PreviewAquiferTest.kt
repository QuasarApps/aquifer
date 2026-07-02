package io.github.quasarapps.aquifer.compose

import app.cash.turbine.test
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Origin
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PreviewAquiferTest {

    @Test
    fun `seeded keys stream content and missing keys stream empty`() = runTest {
        val store = previewAquifer("u1" to "Ada")

        store.stream("u1").test {
            assertEquals(DataState.Content("Ada", Origin.MEMORY, isStale = false), awaitItem())
        }
        store.stream("nope").test {
            // Previews never fetch, so a missing key is an affirmative empty state.
            assertEquals(DataState.Empty, awaitItem())
        }
    }

    @Test
    fun `puts and invalidations update active streams`() = runTest {
        val store = previewAquifer("u1" to "Ada")

        store.stream("u1").test {
            assertEquals(DataState.Content("Ada", Origin.MEMORY, isStale = false), awaitItem())

            store.put("u1", "Edited")
            assertEquals(DataState.Content("Edited", Origin.MEMORY, isStale = false), awaitItem())

            store.invalidate("u1")
            assertEquals(DataState.Empty, awaitItem())
        }
    }

    @Test
    fun `get serves seeded values and never fetches`() = runTest {
        val store = previewAquifer("u1" to "Ada")

        assertEquals("Ada", store.get("u1"))
        assertEquals("Ada", store.fresh("u1"))
        assertFailsWith<CacheMissException> { store.get("missing") }
    }

    @Test
    fun `streamMany combines seeded content and missing keys into one map`() = runTest {
        val store = previewAquifer("u1" to "Ada", "u2" to "Bob")

        store.streamMany(setOf("u1", "u2", "missing")).test {
            assertEquals(
                mapOf(
                    "u1" to DataState.Content("Ada", Origin.MEMORY, isStale = false),
                    "u2" to DataState.Content("Bob", Origin.MEMORY, isStale = false),
                    "missing" to DataState.Empty, // previews never fetch: absence is an affirmative empty
                ),
                awaitItem(),
            )
        }
    }
}
