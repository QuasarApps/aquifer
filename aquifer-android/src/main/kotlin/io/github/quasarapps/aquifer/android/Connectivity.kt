package io.github.quasarapps.aquifer.android

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import io.github.quasarapps.aquifer.Aquifer
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow

/**
 * Emits once each time internet connectivity is *restored* — that is, when a network with
 * [NetworkCapabilities.NET_CAPABILITY_INTERNET] becomes available after a period with none.
 *
 * Connectivity that is already present when collection starts does not emit, and switching
 * between networks (Wi-Fi ↔ cellular) without fully losing connectivity does not emit either:
 * the flow fires only for offline → online transitions. A device that is *offline* when
 * collection starts emits as soon as connectivity first appears.
 *
 * The underlying [ConnectivityManager.NetworkCallback] is registered while the flow is
 * collected and unregistered when collection stops. Callbacks may arrive on a system thread;
 * the flow is safe to collect from any dispatcher. The `ACCESS_NETWORK_STATE` permission this
 * requires is declared in this library's manifest and merged into your app automatically.
 *
 * This is the canonical trigger for [Aquifer.revalidateOn] — see [revalidateOnReconnect].
 */
public fun Context.connectivityRestoredFlow(): Flow<Unit> = callbackFlow {
    val manager = applicationContext.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    val request = NetworkRequest.Builder()
        .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
        .build()

    val callback = object : ConnectivityManager.NetworkCallback() {
        private val available = mutableSetOf<Network>()

        // A device that is offline when collection starts receives no initial callbacks at
        // all, so the "we have been offline" flag must be seeded from a snapshot. Only the
        // flag is seeded: snapshot networks never produce a matching onLost for this
        // callback, so seeding `available` could park phantom entries there forever.
        private var offlineObserved = !manager.isCurrentlyOnline()

        override fun onAvailable(network: Network) {
            val restored = synchronized(this) {
                available += network
                offlineObserved.also { offlineObserved = false }
            }
            if (restored) trySend(Unit)
        }

        override fun onLost(network: Network) {
            synchronized(this) {
                available -= network
                if (available.isEmpty()) offlineObserved = true
            }
        }
    }

    manager.registerNetworkCallback(request, callback)
    awaitClose { manager.unregisterNetworkCallback(callback) }
}

/** Snapshot of whether any network currently offers internet capability. */
@Suppress("DEPRECATION") // allNetworks: required for the minSdk 21 snapshot; callbacks take over after registration.
private fun ConnectivityManager.isCurrentlyOnline(): Boolean =
    allNetworks.any { network ->
        getNetworkCapabilities(network)?.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) == true
    }

/**
 * Refreshes this store's stale, actively observed keys whenever internet connectivity is
 * restored, for the lifetime of the store:
 *
 * ```
 * val users = aquifer<UserId, User> { fetcher { api.fetchUser(it) } }
 * users.revalidateOnReconnect(context)
 * ```
 *
 * Equivalent to `revalidateOn(context.connectivityRestoredFlow())`; see [Aquifer.revalidateOn]
 * and [Aquifer.revalidateActive] for the exact semantics (fresh entries are skipped, fetches
 * are shared, only fetch-capable streams count as active).
 */
public fun <K : Any, V : Any> Aquifer<K, V>.revalidateOnReconnect(context: Context) {
    revalidateOn(context.applicationContext.connectivityRestoredFlow())
}
