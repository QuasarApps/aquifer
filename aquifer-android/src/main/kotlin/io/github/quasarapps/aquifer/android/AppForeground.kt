package io.github.quasarapps.aquifer.android

import android.os.Handler
import android.os.Looper
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ProcessLifecycleOwner
import io.github.quasarapps.aquifer.Aquifer
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

/**
 * Emits once each time [lifecycle] moves up into the started state — by default the process
 * lifecycle, i.e. the app coming to the foreground.
 *
 * A lifecycle that is already started when collection begins does not emit (the replayed
 * initial `ON_START` is suppressed); a lifecycle that is *below* started at subscription
 * emits on its next start. Note the launch nuance this implies: wiring up before the app
 * first becomes visible (e.g. in `Application.onCreate`) yields an emission when the first
 * screen appears — for [revalidateOnAppForeground] that early trigger is harmless, since
 * refreshes are shared with (or subsumed by) the screen's own initial loads.
 *
 * The observer is added while the flow is collected and removed when collection stops;
 * lifecycle access is marshalled to the main thread as the lifecycle API requires.
 */
public fun appForegroundedFlow(
    lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle,
): Flow<Unit> = callbackFlow {
    // Touched only on the main thread, where all lifecycle callbacks arrive.
    var backgroundObserved = false
    val observer = LifecycleEventObserver { _, event ->
        when (event) {
            Lifecycle.Event.ON_STOP -> backgroundObserved = true
            Lifecycle.Event.ON_START -> if (backgroundObserved) {
                backgroundObserved = false
                trySend(Unit)
            }

            else -> Unit
        }
    }

    onMainThread {
        // A lifecycle below STARTED at subscription would otherwise never arm the flag —
        // no ON_STOP is coming — and its first genuine foreground entry would be missed.
        backgroundObserved = !lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED)
        lifecycle.addObserver(observer)
    }
    awaitClose { onMainThread { lifecycle.removeObserver(observer) } }
}

/**
 * Refreshes this store's stale, actively observed keys whenever the app returns to the
 * foreground, for the lifetime of the store:
 *
 * ```
 * articles.revalidateOnAppForeground()
 * ```
 *
 * Equivalent to `revalidateOn(appForegroundedFlow())`; see [Aquifer.revalidateOn] and
 * [Aquifer.revalidateActive] for the exact semantics.
 */
public fun <K : Any, V : Any> Aquifer<K, V>.revalidateOnAppForeground(
    lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle,
) {
    revalidateOn(appForegroundedFlow(lifecycle))
}

/** Runs [block] immediately when already on the main thread, otherwise posts it there. */
private fun onMainThread(block: () -> Unit) {
    if (Looper.myLooper() == Looper.getMainLooper()) {
        block()
    } else {
        Handler(Looper.getMainLooper()).post(block)
    }
}
