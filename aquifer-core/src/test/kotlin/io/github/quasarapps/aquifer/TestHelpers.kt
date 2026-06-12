package io.github.quasarapps.aquifer

import kotlinx.coroutines.yield

/**
 * Suspends the test coroutine long enough for work already scheduled on the store's
 * (background) scope to run to completion.
 *
 * `runTest` only executes background work while the test coroutine itself is suspended, so
 * fire-and-forget effects — like a stale-while-revalidate refresh — need an explicit
 * suspension point before they can be asserted on.
 */
suspend fun settle() {
    repeat(8) { yield() }
}
