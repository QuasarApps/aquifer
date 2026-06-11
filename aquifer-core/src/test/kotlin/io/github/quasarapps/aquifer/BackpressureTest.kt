package io.github.quasarapps.aquifer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

/** A [SourceOfTruth] whose read of one key hangs until released. */
private class GatedSourceOfTruth(private val slowKey: String) : SourceOfTruth<String, Int> {

    val gate = CompletableDeferred<Unit>()

    override suspend fun read(key: String): PersistedEntry<Int>? {
        if (key == slowKey) gate.await()
        return null
    }

    override suspend fun write(key: String, entry: PersistedEntry<Int>) = Unit
    override suspend fun delete(key: String) = Unit
    override suspend fun deleteAll() = Unit
}

class BackpressureTest {

    @Test
    fun `a stalled stream collector does not block writers or other callers`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
        }
        store.put("k", 0)

        // A collector that hangs forever on its first item: its downstream applies maximal
        // backpressure, which must not propagate into the store's update bus.
        val gate = CompletableDeferred<Unit>()
        backgroundScope.launch {
            store.stream("k").collect { gate.await() }
        }
        settle() // The collector is now subscribed and stuck.

        // Far more writes than any internal buffer; these must complete without suspending
        // on the stalled collector. The timeout only fires if a put hangs.
        withTimeout(5_000) {
            repeat(500) { store.put("k", it) }
        }

        assertEquals(499, store.get("k", Freshness.CacheOnly))
        // Fetches for unrelated keys complete as well — the engine is not stalled.
        assertEquals(-1, store.get("unrelated"))
    }

    @Test
    fun `a slow persistence read while a stream starts does not block writers`() = runTest {
        val disk = GatedSourceOfTruth(slowKey = "slow")
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            fetcher { -1 }
            persistence(disk)
        }
        store.put("hot", 0)

        // This stream's storage hydration hangs; it must do so BEFORE subscribing to the
        // update bus, or its unconsumed subscription would backpressure every emitter.
        val stalled = backgroundScope.launch { store.stream("slow").collect { } }
        settle()

        withTimeout(5_000) {
            repeat(200) { store.put("hot", it) }
        }
        assertEquals(199, store.get("hot", Freshness.CacheOnly))

        disk.gate.complete(Unit)
        settle()
        stalled.cancel()
    }
}
