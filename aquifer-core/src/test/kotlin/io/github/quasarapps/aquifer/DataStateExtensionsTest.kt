package io.github.quasarapps.aquifer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DataStateExtensionsTest {

    private val boom = RuntimeException("boom")

    @Test
    fun `isLoading is true only for loading states`() {
        assertTrue(DataState.Loading(null).isLoading)
        assertTrue(DataState.Loading(1).isLoading)
        assertFalse(DataState.Content(1, Origin.MEMORY).isLoading)
        assertFalse(DataState.Failure<Int>(boom).isLoading)
    }

    @Test
    fun `valueOrThrow returns carried values`() {
        assertEquals(1, DataState.Content(1, Origin.FETCHER).valueOrThrow())
        assertEquals(1, DataState.Loading(1).valueOrThrow())
    }

    @Test
    fun `valueOrThrow rethrows the failure's own error`() {
        val thrown = assertFailsWith<RuntimeException> {
            DataState.Failure<Int>(boom, value = 1).valueOrThrow()
        }
        assertSame(boom, thrown)
    }

    @Test
    fun `valueOrThrow on an empty loading state reports no value`() {
        assertFailsWith<NoSuchElementException> { DataState.Loading<Int>(null).valueOrThrow() }
    }

    @Test
    fun `map preserves shape, origin, staleness, and errors`() {
        assertEquals(
            DataState.Content("1!", Origin.PERSISTENCE, isStale = true),
            DataState.Content(1, Origin.PERSISTENCE, isStale = true).map { "$it!" },
        )
        assertEquals(DataState.Loading("1!"), DataState.Loading(1).map { "$it!" })
        assertEquals(DataState.Loading<String>(null), DataState.Loading<Int>(null).map { "$it!" })
        assertEquals(DataState.Failure(boom, "1!"), DataState.Failure(boom, 1).map { "$it!" })
        assertNull(DataState.Failure<Int>(boom).map { "$it!" }.value)
    }

    @Test
    fun `onContent and onFailure fire only for their states`() {
        val seen = mutableListOf<String>()

        DataState.Content(1, Origin.MEMORY)
            .onContent { seen += "content:$it" }
            .onFailure { seen += "failure" }
        DataState.Failure<Int>(boom, value = 2)
            .onContent { seen += "content:$it" }
            .onFailure { seen += "failure:${it.message}" }
        DataState.Loading<Int>(3)
            .onContent { seen += "content:$it" }
            .onFailure { seen += "failure" }

        assertEquals(listOf("content:1", "failure:boom"), seen)
    }
}
