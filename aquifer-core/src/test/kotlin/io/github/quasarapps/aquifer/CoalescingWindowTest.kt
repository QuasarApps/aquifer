package io.github.quasarapps.aquifer

import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

/** DataLoader-style coalescing: individual fetches within the window collapse into one call. */
class CoalescingWindowTest {

    @Test
    fun `concurrent single-key fetches within the window coalesce into one call`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher(coalesceWindow = 10.milliseconds) { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        val a = async { store.get("a") }
        val b = async { store.get("bb") }
        val c = async { store.get("ccc") }
        settle() // all three enqueue into the same window before it elapses

        assertEquals(1, a.await())
        assertEquals(2, b.await())
        assertEquals(3, c.await())
        assertEquals(listOf(setOf("a", "bb", "ccc")), batches, "one call for the whole window")
    }

    @Test
    fun `a lone fetch still resolves after the window as a batch of one`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher(coalesceWindow = 10.milliseconds) { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        assertEquals(3, store.get("abc"))
        assertEquals(listOf(setOf("abc")), batches)
    }

    @Test
    fun `maxBatchSize dispatches early, the overflow waits for the next window`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher(coalesceWindow = 1.minutes, maxBatchSize = 2) { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        val a = async { store.get("a") }
        val b = async { store.get("bb") }
        val c = async { store.get("ccc") }
        settle() // a + bb hit maxBatchSize and dispatch at once; ccc opens a fresh window

        assertEquals(1, a.await())
        assertEquals(2, b.await())
        assertEquals(listOf(setOf("a", "bb")), batches, "the full batch fired without waiting")

        assertEquals(3, c.await()) // advances past the window for the overflow key
        assertEquals(listOf(setOf("a", "bb"), setOf("ccc")), batches)
    }

    @Test
    fun `getAll dispatches its keys immediately, not waiting for the window`() = runTest {
        val batches = mutableListOf<Set<String>>()
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher(coalesceWindow = 1.minutes) { keys ->
                batches += keys
                keys.associateWith { it.length }
            }
        }

        // No virtual-time advance happens here: getAll has its keys already and batches now.
        assertEquals(mapOf("a" to 1, "bb" to 2), store.getAll(setOf("a", "bb")))
        assertEquals(listOf(setOf("a", "bb")), batches)
    }

    @Test
    fun `a coalesced fetch failure is retried into the next window`() = runTest {
        var attempts = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            retry {
                maxAttempts = 2
                initialDelay = 1.milliseconds
            }
            batchFetcher(coalesceWindow = 5.milliseconds) { keys ->
                attempts++
                if (attempts == 1) throw IOException("transient") else keys.associateWith { it.length }
            }
        }

        // First window's batch fails; the per-key retry re-enters the next window and succeeds.
        assertEquals(1, store.get("a"))
        assertEquals(2, attempts)
    }

    @Test
    fun `a missing key in a coalesced batch fails only that key`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher(coalesceWindow = 10.milliseconds) { keys ->
                keys.filter { it != "miss" }.associateWith { it.length }
            }
        }

        val ok = async { store.get("a") }
        val gone = async { runCatching { store.get("miss") } }
        settle()

        assertEquals(1, ok.await())
        assertFailsWith<BatchKeyMissingException> { gone.await().getOrThrow() }
    }

    @Test
    fun `window and batch-size are validated`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                batchFetcher(coalesceWindow = (-1).milliseconds) { keys -> keys.associateWith { 1 } }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, Int> {
                batchFetcher(maxBatchSize = 0) { keys -> keys.associateWith { 1 } }
            }
        }
    }
}
