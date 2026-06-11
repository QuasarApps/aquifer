package io.github.quasarapps.aquifer

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

class AquiferBuilderTest {

    @Test
    fun `a fetcher is required`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> { }
        }
    }

    @Test
    fun `max entries must be positive`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "value" }
                memoryCache { maxEntries = 0 }
            }
        }
    }

    @Test
    fun `time to live must be positive`() {
        assertFailsWith<IllegalArgumentException> {
            aquifer<String, String> {
                fetcher { "value" }
                freshness { timeToLive = (-1).minutes }
            }
        }
    }
}
