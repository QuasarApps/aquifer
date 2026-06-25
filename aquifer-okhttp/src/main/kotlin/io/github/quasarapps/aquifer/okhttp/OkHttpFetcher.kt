package io.github.quasarapps.aquifer.okhttp

import okhttp3.Call
import okhttp3.Request
import okhttp3.ResponseBody

/**
 * Builds a plain (non-conditional) [fetcher][io.github.quasarapps.aquifer.AquiferBuilder.fetcher]
 * over OkHttp, for backends that don't support HTTP revalidation (no `ETag`/`Last-Modified`).
 * Each fetch issues [request]'s call and, on a 2xx, hands the body to [parse]. Reach for
 * [okHttpConditionalFetcher] instead when the backend speaks ETags and you want a 304 to
 * re-age the cache without re-downloading.
 *
 * ```
 * val users = aquifer<UserId, User> {
 *     fetcher(
 *         okHttpFetcher(
 *             callFactory = client,
 *             request = { id -> Request.Builder().url("$BASE/users/$id").build() },
 *             parse = { _, body -> json.decodeFromString<User>(body.string()) },
 *         )
 *     )
 *     freshness { timeToLive = 5.minutes }
 * }
 * ```
 *
 * Semantics:
 * - A 2xx response is decoded by [parse].
 * - Any non-2xx status throws [HttpException] (an `IOException`) carrying the response
 *   [code][HttpException.code], so a retry/negative-cache policy can branch on the status
 *   (e.g. retry only 5xx); it flows through Aquifer's normal failure path (retry policy,
 *   `DataState.Failure`, stale-if-error) by default.
 * - The call is cancelled if the fetch's coroutine is cancelled, and the response body is
 *   always closed.
 *
 * @param callFactory typically an [okhttp3.OkHttpClient]; the seam also accepts any
 *   [Call.Factory] (interceptors, test fakes).
 * @param request builds the call for a key.
 * @param parse decodes a successful response's body into a value. Runs inside the fetch, so
 *   thrown exceptions fail the fetch normally.
 */
public fun <K : Any, V : Any> okHttpFetcher(
    callFactory: Call.Factory,
    request: (key: K) -> Request,
    parse: suspend (key: K, body: ResponseBody) -> V,
): suspend (key: K) -> V = { key ->
    callFactory.newCall(request(key)).await().use { response ->
        if (!response.isSuccessful) {
            throw HttpException(response.code, response.request.url.toString())
        }
        val body = checkNotNull(response.body) {
            "HTTP ${response.code} from ${response.request.url} had no body"
        }
        parse(key, body)
    }
}
