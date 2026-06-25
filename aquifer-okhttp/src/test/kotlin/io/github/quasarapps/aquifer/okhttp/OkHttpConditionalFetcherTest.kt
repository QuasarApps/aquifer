package io.github.quasarapps.aquifer.okhttp

import io.github.quasarapps.aquifer.FetchResult
import kotlinx.coroutines.test.runTest
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class OkHttpConditionalFetcherTest {

    private lateinit var server: MockWebServer
    private val client = OkHttpClient()

    private val fetcher = okHttpConditionalFetcher<String, String>(
        callFactory = client,
        request = { key -> Request.Builder().url(server.url("/items/$key")).build() },
        parse = { _, body -> body.string() },
    )

    private val cacheAwareFetcher = okHttpConditionalFetcher<String, String>(
        callFactory = client,
        request = { key -> Request.Builder().url(server.url("/items/$key")).build() },
        parse = { _, body -> body.string() },
        respectCacheControl = true,
    )

    private suspend fun freshFor(): Duration? =
        assertIs<FetchResult.Fresh<String>>(cacheAwareFetcher("k", null)).freshFor

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
    fun `a 200 with an ETag becomes Fresh and replays If-None-Match`() = runTest {
        server.enqueue(MockResponse().setBody("v1").setHeader("ETag", "\"abc\""))
        server.enqueue(MockResponse().setResponseCode(304))

        val first = assertIs<FetchResult.Fresh<String>>(fetcher("k", null))
        assertEquals("v1", first.value)
        assertNull(server.takeRequest().getHeader("If-None-Match"))

        val second = fetcher("k", first.validator)
        assertEquals(FetchResult.NotModified, second)
        assertEquals("\"abc\"", server.takeRequest().getHeader("If-None-Match"))
    }

    @Test
    fun `last-modified is captured and replayed as If-Modified-Since`() = runTest {
        val stamp = "Wed, 21 Oct 2015 07:28:00 GMT"
        server.enqueue(MockResponse().setBody("v1").setHeader("Last-Modified", stamp))
        server.enqueue(MockResponse().setResponseCode(304))

        val first = assertIs<FetchResult.Fresh<String>>(fetcher("k", null))
        fetcher("k", first.validator)

        server.takeRequest() // the unconditional first request
        val revalidation = server.takeRequest()
        assertEquals(stamp, revalidation.getHeader("If-Modified-Since"))
        assertNull(revalidation.getHeader("If-None-Match"))
    }

    @Test
    fun `both headers are kept and replayed together`() = runTest {
        val stamp = "Wed, 21 Oct 2015 07:28:00 GMT"
        server.enqueue(
            MockResponse().setBody("v1").setHeader("ETag", "\"abc\"").setHeader("Last-Modified", stamp),
        )
        server.enqueue(MockResponse().setResponseCode(304))

        val first = assertIs<FetchResult.Fresh<String>>(fetcher("k", null))
        fetcher("k", first.validator)

        server.takeRequest()
        val revalidation = server.takeRequest()
        assertEquals("\"abc\"", revalidation.getHeader("If-None-Match"))
        assertEquals(stamp, revalidation.getHeader("If-Modified-Since"))
    }

    @Test
    fun `a response without validators yields a null validator`() = runTest {
        server.enqueue(MockResponse().setBody("v1"))

        val result = assertIs<FetchResult.Fresh<String>>(fetcher("k", null))
        assertNull(result.validator, "no ETag/Last-Modified means the next fetch is unconditional")
    }

    @Test
    fun `an error status throws a typed HttpException carrying the status code`() = runTest {
        server.enqueue(MockResponse().setResponseCode(500).setBody("boom"))

        val failure = assertFailsWith<HttpException> { fetcher("k", null) }
        assertEquals(500, failure.code)
        assertIs<IOException>(failure) // still flows through the normal fetch-failure path
        assertEquals(true, failure.message?.contains("HTTP 500"))
    }

    @Test
    fun `a 404 is reported with its status code so a policy can treat it as terminal`() = runTest {
        server.enqueue(MockResponse().setResponseCode(404))

        val failure = assertFailsWith<HttpException> { fetcher("k", null) }
        assertEquals(404, failure.code)
    }

    @Test
    fun `caller-set conditional headers are stripped from unconditional fetches`() = runTest {
        server.enqueue(MockResponse().setBody("v1"))
        val staleHeaders = okHttpConditionalFetcher<String, String>(
            callFactory = client,
            request = { key ->
                Request.Builder()
                    .url(server.url("/items/$key"))
                    .header("If-None-Match", "\"stale\"")
                    .header("If-Modified-Since", "Wed, 21 Oct 2015 07:28:00 GMT")
                    .build()
            },
            parse = { _, body -> body.string() },
        )

        staleHeaders("k", null) // unconditional: the validator is the only source of truth

        val sent = server.takeRequest()
        assertNull(sent.getHeader("If-None-Match"))
        assertNull(sent.getHeader("If-Modified-Since"))
    }

    @Test
    fun `a bare foreign validator is sent as an ETag`() = runTest {
        server.enqueue(MockResponse().setResponseCode(304))

        fetcher("k", "\"foreign\"") // no separator: treated as a bare ETag, best effort

        assertEquals("\"foreign\"", server.takeRequest().getHeader("If-None-Match"))
    }

    @Test
    fun `Cache-Control max-age becomes freshFor when respected`() = runTest {
        server.enqueue(MockResponse().setBody("v").setHeader("Cache-Control", "max-age=300"))

        assertEquals(300.seconds, freshFor())
    }

    @Test
    fun `Age is subtracted from max-age`() = runTest {
        server.enqueue(
            MockResponse().setBody("v").setHeader("Cache-Control", "max-age=600").setHeader("Age", "200"),
        )

        assertEquals(400.seconds, freshFor())
    }

    @Test
    fun `max-age=0, no-store and no-cache each map to ZERO (immediately stale)`() = runTest {
        server.enqueue(MockResponse().setBody("v").setHeader("Cache-Control", "max-age=0"))
        assertEquals(Duration.ZERO, freshFor())

        server.enqueue(MockResponse().setBody("v").setHeader("Cache-Control", "no-store"))
        assertEquals(Duration.ZERO, freshFor())

        server.enqueue(MockResponse().setBody("v").setHeader("Cache-Control", "no-cache"))
        assertEquals(Duration.ZERO, freshFor())
    }

    @Test
    fun `Expires is a fallback measured against the Date header`() = runTest {
        server.enqueue(
            MockResponse().setBody("v")
                .setHeader("Date", "Mon, 01 Jan 2024 00:00:00 GMT")
                .setHeader("Expires", "Mon, 01 Jan 2024 00:02:00 GMT"),
        )

        assertEquals(120.seconds, freshFor())
    }

    @Test
    fun `an already-past Expires is ZERO`() = runTest {
        server.enqueue(
            MockResponse().setBody("v")
                .setHeader("Date", "Mon, 01 Jan 2024 00:02:00 GMT")
                .setHeader("Expires", "Mon, 01 Jan 2024 00:00:00 GMT"),
        )

        assertEquals(Duration.ZERO, freshFor())
    }

    @Test
    fun `no freshness headers yield a null freshFor`() = runTest {
        server.enqueue(MockResponse().setBody("v"))

        assertNull(freshFor())
    }

    @Test
    fun `freshFor stays null when respectCacheControl is off`() = runTest {
        server.enqueue(MockResponse().setBody("v").setHeader("Cache-Control", "max-age=300"))

        // The default fetcher does not opt in, so server freshness is ignored.
        assertNull(assertIs<FetchResult.Fresh<String>>(fetcher("k", null)).freshFor)
    }
}
