package io.github.quasarapps.aquifer.okhttp

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Response
import java.io.IOException
import kotlin.coroutines.resumeWithException

/**
 * Suspends until this [Call] completes, returning its [Response] or throwing the `IOException`
 * OkHttp reports on network failure. The call is enqueued on OkHttp's dispatcher (it never
 * blocks the calling thread). If the awaiting coroutine is cancelled, the call is cancelled and
 * the await fails with `CancellationException` (not the `IOException`); a response that races in
 * after cancellation is closed so the connection is not leaked.
 *
 * This is the bridge [okHttpFetcher] and [okHttpConditionalFetcher] use internally, exposed so
 * a hand-rolled fetcher can `await()` a [Call] without re-implementing the suspend/cancellation
 * plumbing. The caller owns the returned [Response] and must close it (e.g. with `use { }`).
 */
public suspend fun Call.await(): Response = suspendCancellableCoroutine { continuation ->
    enqueue(
        object : Callback {
            override fun onResponse(call: Call, response: Response) {
                continuation.resume(response) { _, resource, _ -> resource.close() }
            }

            override fun onFailure(call: Call, e: IOException) {
                // OkHttp can deliver onFailure after the coroutine was cancelled (a cancelled
                // call surfaces as an IOException). Only resume while still active so that
                // never races a second completion onto the continuation.
                if (continuation.isActive) continuation.resumeWithException(e)
            }
        },
    )
    continuation.invokeOnCancellation { cancel() }
}
