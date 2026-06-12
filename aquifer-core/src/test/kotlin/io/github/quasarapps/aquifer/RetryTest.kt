package io.github.quasarapps.aquifer

import app.cash.turbine.test
import io.github.quasarapps.aquifer.internal.RetryPolicy
import kotlinx.coroutines.test.runTest
import java.io.IOException
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RetryTest {

    @Test
    fun `failures are not retried by default`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                throw IOException("flaky")
            }
        }

        assertFailsWith<IOException> { store.fresh("k") }
        assertEquals(1, calls)
    }

    @Test
    fun `a flaky fetch succeeds within the configured attempts`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                if (calls < 3) throw IOException("flaky #$calls")
                "recovered"
            }
            retry {
                maxAttempts = 3
                initialDelay = 250.milliseconds
                jitter = 0.0
            }
        }

        val startedAt = testScheduler.currentTime
        assertEquals("recovered", store.get("k"))
        assertEquals(3, calls)
        // Exponential backoff without jitter: 250ms + 500ms.
        assertEquals(750, testScheduler.currentTime - startedAt)
    }

    @Test
    fun `attempts are exhausted and the last error propagates`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                throw IOException("down #$calls")
            }
            retry {
                maxAttempts = 3
                jitter = 0.0
            }
        }

        val failure = assertFailsWith<IOException> { store.fresh("k") }
        assertEquals("down #3", failure.message)
        assertEquals(3, calls)
    }

    @Test
    fun `the retryOn predicate filters which failures retry`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                error("not transient")
            }
            retry {
                maxAttempts = 5
                retryOn = { it is IOException }
            }
        }

        assertFailsWith<IllegalStateException> { store.fresh("k") }
        assertEquals(1, calls)
    }

    @Test
    fun `delays cap at maxDelay`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                throw IOException("down")
            }
            retry {
                maxAttempts = 4
                initialDelay = 1.seconds
                multiplier = 10.0
                maxDelay = 5.seconds
                jitter = 0.0
            }
        }

        val startedAt = testScheduler.currentTime
        assertFailsWith<IOException> { store.fresh("k") }
        assertEquals(4, calls)
        // 1s, then 10s and 100s both capped to 5s.
        assertEquals(11_000, testScheduler.currentTime - startedAt)
    }

    @Test
    fun `streams observe one loading and one terminal failure across a retry cycle`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                throw IOException("down #$calls")
            }
            retry {
                maxAttempts = 3
                jitter = 0.0
            }
        }

        store.stream("k").test {
            assertEquals(DataState.Loading(null), awaitItem())
            val failure = awaitItem()
            assertTrue(failure is DataState.Failure && failure.error.message == "down #3")
            expectNoEvents()
        }
        assertEquals(3, calls)
    }

    @Test
    fun `a throwing retryOn predicate means no retry`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                throw IOException("down")
            }
            retry {
                maxAttempts = 5
                retryOn = { error("predicate bug") }
            }
        }

        assertFailsWith<IOException> { store.fresh("k") }
        assertEquals(1, calls)
    }

    @Test
    fun `jitter only ever shortens delays`() {
        val policy = RetryPolicy(
            maxAttempts = 5,
            initialDelay = 1.seconds,
            maxDelay = 30.seconds,
            multiplier = 2.0,
            jitter = 0.5,
            retryOn = { true },
            random = Random(42),
        )

        repeat(200) {
            val delay = policy.delayAfter(attempt = 2, failure = IOException())
            requireNotNull(delay)
            assertTrue(delay <= 2.seconds, "jittered delay $delay must not exceed the exponential value")
            assertTrue(delay >= 1.seconds, "with jitter 0.5 the delay keeps at least half the exponential value")
        }
    }

    @Test
    fun `no delay is produced once attempts are exhausted`() {
        val policy = RetryPolicy(
            maxAttempts = 2,
            initialDelay = 1.seconds,
            maxDelay = 30.seconds,
            multiplier = 2.0,
            jitter = 0.0,
            retryOn = { true },
        )

        assertEquals(1.seconds, policy.delayAfter(1, IOException()))
        assertNull(policy.delayAfter(2, IOException()))
    }

    @Test
    fun `retry configuration is validated`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "v" }
                retry { maxAttempts = 0 }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "v" }
                retry { jitter = 1.5 }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "v" }
                retry { multiplier = 0.5 }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "v" }
                retry { initialDelay = Duration.INFINITE }
            }
        }
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "v" }
                retry { maxDelay = Duration.INFINITE }
            }
        }
    }
}
