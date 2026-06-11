package io.github.quasarapps.aquifer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class AquiferLifecycleTest {

    @Test
    fun `operations after close throw`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "value" }
        }

        store.close()

        assertFailsWith<IllegalStateException> { store.get("k") }
        assertFailsWith<IllegalStateException> { store.stream("k") }
        assertFailsWith<IllegalStateException> { store.put("k", "v") }
        assertFailsWith<IllegalStateException> { store.invalidate("k") }
        assertFailsWith<IllegalStateException> { store.invalidateAll() }
    }

    @Test
    fun `close is idempotent`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "value" }
        }

        store.close()
        store.close()
    }

    @Test
    fun `close fails in-flight gets with a real error, not a silent cancellation`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(10_000)
                "too late"
            }
        }

        val pending = async { runCatching { store.get("k") } }
        settle() // The fetch is now in flight.

        store.close()

        // The caller's coroutine was NOT cancelled — it received a visible error.
        assertIs<AquiferException>(pending.await().exceptionOrNull())
    }

    @Test
    fun `cancelling the caller does not cancel the shared fetch`() = runTest {
        var completions = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                delay(100)
                completions++
                "value"
            }
        }

        val impatient = async { store.get("k") }
        settle() // The fetch is now in flight.
        impatient.cancel()

        // The fetch keeps running in the store's scope and lands in the cache.
        delay(200)
        assertEquals(1, completions)
        assertEquals("value", store.get("k", Freshness.CacheOnly))
    }

    @Test
    fun `cancelling the parent scope stops the store's work and closes the store`() = runTest {
        val parent = CoroutineScope(SupervisorJob() + StandardTestDispatcher(testScheduler))
        var completions = 0
        val store = aquifer<String, String> {
            scope(parent)
            fetcher {
                delay(10_000)
                completions++
                "value"
            }
        }

        val pending = async { runCatching { store.get("k") } }
        settle() // The fetch is now in flight.

        parent.cancel()

        assertIs<AquiferException>(pending.await().exceptionOrNull())
        assertEquals(0, completions)

        // The store reports itself closed: operations fail fast instead of hanging forever.
        settle()
        assertFailsWith<IllegalStateException> { store.get("another") }
        assertFailsWith<IllegalStateException> { store.stream("another") }
    }

    @Test
    fun `a store without an explicit scope manages its own lifecycle`() {
        val store = aquifer<String, String> {
            fetcher { "value" }
        }
        store.close()
    }
}
