package io.github.quasarapps.aquifer.okhttp

import java.io.IOException

/**
 * Thrown by [okHttpConditionalFetcher] when a response is neither a success (2xx) nor a
 * `304 Not Modified` — i.e. a 4xx/5xx or other non-success status. It carries the HTTP [code]
 * so Aquifer's resilience policies can branch on the status instead of a flattened string:
 * for example retry only server errors
 * (`retry { retryOn = { it is HttpException && it.code in 500..599 } }`), or treat a `404`
 * as a terminal miss rather than a retryable error.
 *
 * It extends [IOException], so with no extra configuration it flows through Aquifer's normal
 * fetch-failure path (retry policy, `DataState.Failure`, stale-if-error) exactly like any
 * other I/O failure.
 *
 * @property code the HTTP status code of the offending response.
 * @property url the request URL that produced it, included for diagnostics.
 */
public class HttpException(
    public val code: Int,
    public val url: String,
) : IOException("HTTP $code fetching $url")
