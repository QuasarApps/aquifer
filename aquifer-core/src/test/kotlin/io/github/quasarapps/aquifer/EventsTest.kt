package io.github.quasarapps.aquifer

import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Records every callback as a readable line for whole-sequence assertions. */
private class RecordingEvents : AquiferEvents<String> {

    val log = mutableListOf<String>()

    override fun onFetchStarted(key: String) {
        log += "started($key)"
    }

    override fun onFetchSucceeded(key: String, duration: Duration) {
        log += "succeeded($key, $duration)"
    }

    override fun onFetchRetried(key: String, attempt: Int, error: Throwable, nextDelay: Duration) {
        log += "retried($key, attempt=$attempt, ${error.message})"
    }

    override fun onFetchFailed(key: String, error: Throwable, attempts: Int) {
        log += "failed($key, ${error.message}, attempts=$attempts)"
    }

    override fun onPersistenceWriteFailed(key: String, error: Throwable) {
        log += "persistenceWriteFailed($key, ${error.message})"
    }
}

class EventsTest {

    @Test
    fun `successful fetches report start and duration`() = runTest {
        val recorder = RecordingEvents()
        val clock = FakeClock()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(clock)
            events(recorder)
            fetcher {
                clock.advanceBy(2.seconds)
                "value"
            }
        }

        store.get("k")

        assertEquals(listOf("started(k)", "succeeded(k, 2s)"), recorder.log)
    }

    @Test
    fun `terminal failures report the attempt count`() = runTest {
        val recorder = RecordingEvents()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            events(recorder)
            fetcher { throw IOException("down") }
        }

        assertFailsWith<IOException> { store.fresh("k") }

        assertEquals(listOf("started(k)", "failed(k, down, attempts=1)"), recorder.log)
    }

    @Test
    fun `each retry is reported before the terminal outcome`() = runTest {
        val recorder = RecordingEvents()
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(FakeClock())
            events(recorder)
            fetcher {
                calls++
                if (calls < 3) throw IOException("flaky #$calls")
                "recovered"
            }
            retry {
                maxAttempts = 3
                initialDelay = 100.milliseconds
                jitter = 0.0
            }
        }

        store.get("k")

        assertEquals(
            listOf(
                "started(k)",
                "retried(k, attempt=1, flaky #1)",
                "retried(k, attempt=2, flaky #2)",
                "succeeded(k, 0s)",
            ),
            recorder.log,
        )
    }

    @Test
    fun `failed best-effort persistence writes are reported`() = runTest {
        val recorder = RecordingEvents()
        val store = aquifer<String, String> {
            scope(backgroundScope)
            clock(FakeClock())
            events(recorder)
            fetcher { "value" }
            persistence(object : SourceOfTruth<String, String> {
                override suspend fun read(key: String): PersistedEntry<String>? = null
                override suspend fun write(key: String, entry: PersistedEntry<String>) =
                    throw IOException("disk full")

                override suspend fun delete(key: String) = Unit
                override suspend fun deleteAll() = Unit
            })
        }

        assertEquals("value", store.get("k"))
        settle()

        assertEquals(
            listOf("started(k)", "succeeded(k, 0s)", "persistenceWriteFailed(k, disk full)"),
            recorder.log,
        )
    }

    @Test
    fun `a throwing listener never disturbs the engine`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            events(object : AquiferEvents<String> {
                override fun onFetchStarted(key: String) = error("listener bug")
                override fun onFetchSucceeded(key: String, duration: Duration) = error("listener bug")
            })
            fetcher { "value" }
        }

        assertEquals("value", store.get("k"))
    }
}
