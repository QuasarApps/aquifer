package io.github.quasarapps.aquifer

import kotlin.time.Duration

/**
 * The outcome of a conditional fetch (see [AquiferBuilder.conditionalFetcher]).
 *
 * A conditional fetcher receives the cached entry's [validator][PersistedEntry.validator] —
 * an opaque token such as an HTTP `ETag` or `Last-Modified` value — and may answer that the
 * cached value is still current instead of re-downloading it:
 *
 * ```
 * conditionalFetcher { id, validator ->
 *     val response = api.fetchArticle(id, ifNoneMatch = validator)
 *     when {
 *         response.code == 304 -> FetchResult.NotModified
 *         else -> FetchResult.Fresh(response.body(), validator = response.header("ETag"))
 *     }
 * }
 * ```
 */
public sealed interface FetchResult<out V : Any> {

    /**
     * The fetch produced a (new) value. [validator] is stored alongside it — in memory and
     * in persistence — and handed to the next conditional fetch of the key; `null` means
     * the next fetch is unconditional.
     *
     * [freshFor] is the server-derived remaining lifetime of the value (e.g. derived from an
     * HTTP `Cache-Control: max-age`). When set, it is the authoritative freshness horizon —
     * never jittered, and overriding the builder `timeToLive` — and is honored only by a
     * [SourceOfTruth] that persists it (so it survives process restarts). `null` (the default)
     * means the origin expressed no opinion, so the store falls back to its own time-to-live;
     * [Duration.ZERO] declares the value immediately stale on the next read.
     */
    public data class Fresh<V : Any>(
        val value: V,
        val validator: String? = null,
        val freshFor: Duration? = null,
    ) : FetchResult<V>

    /**
     * The cached value is still current (an HTTP 304). The store keeps the value and its
     * validator and refreshes the entry's age, so time-to-live decisions start over without
     * the payload ever crossing the network. Only meaningful when the fetcher was given a
     * non-null validator; returning it otherwise is a contract violation and fails the fetch.
     */
    public data object NotModified : FetchResult<Nothing>
}
