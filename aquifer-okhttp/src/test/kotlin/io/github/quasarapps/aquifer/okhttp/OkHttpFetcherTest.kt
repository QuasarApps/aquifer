package io.github.quasarapps.aquifer.okhttp

import kotlinx.coroutines.test.runTest
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OkHttpFetcherTest {

    private lateinit var server: MockWebServer
    private val client = OkHttpClient()

    private val fetcher = okHttpFetcher<String, String>(
        callFactory = client,
        request = { key -> Request.Builder().url(server.url("/items/$key")).build() },
        parse = { _, body -> body.string() },
    )

    @BeforeEach
    fun start() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun stop() {
        server.shutdown()
    }

    @Test
    fun `a 200 is parsed into the value`() = runTest {
        server.enqueue(MockResponse().setBody("hello"))

        assertEquals("hello", fetcher("k"))
        assertEquals("/items/k", server.takeRequest().path)
    }

    @Test
    fun `a 500 throws a typed HttpException carrying the status code`() = runTest {
        server.enqueue(MockResponse().setResponseCode(500).setBody("boom"))

        val failure = assertFailsWith<HttpException> { fetcher("k") }
        assertEquals(500, failure.code)
    }

    @Test
    fun `a 404 is reported with its status code`() = runTest {
        server.enqueue(MockResponse().setResponseCode(404))

        val failure = assertFailsWith<HttpException> { fetcher("k") }
        assertEquals(404, failure.code)
    }

    @Test
    fun `the public Call await seam returns the response`() = runTest {
        server.enqueue(MockResponse().setBody("v"))

        val call = client.newCall(Request.Builder().url(server.url("/x")).build())
        val body = call.await().use { it.body?.string() }

        assertEquals("v", body)
    }
}
