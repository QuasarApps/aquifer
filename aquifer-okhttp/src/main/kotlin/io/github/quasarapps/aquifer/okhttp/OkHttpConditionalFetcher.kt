package io.github.quasarapps.aquifer.okhttp

import io.github.quasarapps.aquifer.FetchResult
import okhttp3.Call
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import java.net.HttpURLConnection
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Builds a [conditional fetcher][io.github.quasarapps.aquifer.AquiferBuilder.conditionalFetcher]
 * over OkHttp that wires HTTP revalidation automatically: responses' `ETag`/`Last-Modified`
 * headers are captured into the entry's validator, replayed as `If-None-Match`/
 * `If-Modified-Since` on the next fetch of the key, and a 304 becomes
 * [FetchResult.NotModified] — the store re-ages the cached value without the payload ever
 * crossing the network.
 *
 * ```
 * val articles = aquifer<ArticleId, Article> {
 *     conditionalFetcher(
 *         okHttpConditionalFetcher(
 *             callFactory = client,
 *             request = { id -> Request.Builder().url("$BASE/articles/$id").build() },
 *             parse = { _, body -> json.decodeFromString<Article>(body.string()) },
 *         )
 *     )
 *     freshness { timeToLive = 10.minutes }
 * }
 * ```
 *
 * Semantics:
 * - 2xx responses become [FetchResult.Fresh] with [parse]'s value and a validator built from
 *   the response's `ETag` and `Last-Modified` headers (both kept when both are present;
 *   `null` when neither is — the next fetch is then unconditional).
 * - 304 becomes [FetchResult.NotModified].
 * - Any other status throws [HttpException] (an `IOException`) carrying the response
 *   [code][HttpException.code], so a retry/negative-cache policy can branch on the status
 *   (e.g. retry only 5xx). It still flows through Aquifer's normal failure path (retry
 *   policy, `DataState.Failure`, stale-if-error fallbacks) by default.
 * - [request]'s own conditional headers, if any, are replaced by the validator's.
 * - The call is cancelled if the fetch's coroutine is cancelled; response bodies are always
 *   closed. The validator string is an implementation detail of this helper — treat it as
 *   opaque.
 *
 * @param callFactory typically an [okhttp3.OkHttpClient]; the seam also accepts any
 *   [Call.Factory] (interceptors, test fakes).
 * @param request builds the unconditional GET for a key; revalidation headers are added on
 *   top of it.
 * @param parse decodes a successful response's body into a value. Runs inside the fetch, so
 *   thrown exceptions fail the fetch normally.
 * @param respectCacheControl when `true`, a 2xx response's `Cache-Control`/`Expires` headers are
 *   parsed into the entry's [FetchResult.Fresh.freshFor] server lifetime (`max-age` minus `Age`;
 *   `no-store`/`no-cache`/`max-age=0` → immediately stale; `Expires` as a fallback). Off by
 *   default, so the store's own `timeToLive` governs unless you opt in.
 */
public fun <K : Any, V : Any> okHttpConditionalFetcher(
    callFactory: Call.Factory,
    request: (key: K) -> Request,
    parse: suspend (key: K, body: ResponseBody) -> V,
    respectCacheControl: Boolean = false,
): suspend (key: K, validator: String?) -> FetchResult<V> = { key, validator ->
    val conditional = request(key).withValidatorHeaders(validator)
    callFactory.newCall(conditional).await().use { response ->
        when {
            response.code == HttpURLConnection.HTTP_NOT_MODIFIED -> FetchResult.NotModified

            !response.isSuccessful ->
                throw HttpException(response.code, response.request.url.toString())

            else -> {
                val body = checkNotNull(response.body) {
                    "HTTP ${response.code} from ${response.request.url} had no body"
                }
                val freshFor = if (respectCacheControl) parseServerFreshness(response) else null
                FetchResult.Fresh(parse(key, body), validatorOf(response), freshFor)
            }
        }
    }
}

/**
 * The validator packs both revalidation headers as `etag` + '\n' + `last-modified` (header
 * values cannot contain line breaks, so the separator is unambiguous); either half may be
 * empty. A validator from another producer without the separator is treated as a bare ETag.
 */
private const val VALIDATOR_SEPARATOR = '\n'

private fun Request.withValidatorHeaders(validator: String?): Request {
    // Both headers are always stripped first: the validator is the single source of truth
    // for conditionality, so a caller-set header never leaks into an unconditional fetch
    // (and a one-sided validator never leaves the other header behind).
    val builder = newBuilder()
        .removeHeader("If-None-Match")
        .removeHeader("If-Modified-Since")
    if (validator != null) {
        val etag = validator.substringBefore(VALIDATOR_SEPARATOR)
        val lastModified = validator.substringAfter(VALIDATOR_SEPARATOR, missingDelimiterValue = "")
        if (etag.isNotEmpty()) builder.header("If-None-Match", etag)
        if (lastModified.isNotEmpty()) builder.header("If-Modified-Since", lastModified)
    }
    return builder.build()
}

private fun validatorOf(response: Response): String? {
    val etag = response.header("ETag").orEmpty()
    val lastModified = response.header("Last-Modified").orEmpty()
    if (etag.isEmpty() && lastModified.isEmpty()) return null
    return "$etag$VALIDATOR_SEPARATOR$lastModified"
}

/**
 * Derives the server-declared freshness lifetime ([FetchResult.Fresh.freshFor]) from a response's
 * `Cache-Control`/`Expires` headers, or `null` when the origin expressed no opinion. `no-store`,
 * `no-cache`, and `max-age=0` map to [Duration.ZERO] (immediately stale — revalidate on the next
 * read); `max-age` is reduced by any `Age`; `Expires` is consulted only when there is no `max-age`
 * directive, measured against the response `Date` (or its local receipt time when absent). Every
 * result is floored at [Duration.ZERO], and a malformed header yields `null` rather than failing
 * the fetch. Shared-proxy directives (`s-maxage`, `private`, …) are ignored.
 */
private fun parseServerFreshness(response: Response): Duration? = runCatching {
    val cacheControl = response.cacheControl
    when {
        cacheControl.noStore || cacheControl.noCache -> Duration.ZERO
        cacheControl.maxAgeSeconds >= 0 -> {
            val ageSeconds = response.header("Age")?.toLongOrNull() ?: 0L
            (cacheControl.maxAgeSeconds.toLong() - ageSeconds).coerceAtLeast(0L).seconds
        }
        else -> {
            val expiresMillis = response.headers.getDate("Expires")?.time ?: return@runCatching null
            val referenceMillis = response.headers.getDate("Date")?.time ?: response.receivedResponseAtMillis
            (expiresMillis - referenceMillis).coerceAtLeast(0L).milliseconds
        }
    }
}.getOrNull()
