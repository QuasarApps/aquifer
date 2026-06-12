package io.github.quasarapps.aquifer.android

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import androidx.test.core.app.ApplicationProvider
import app.cash.turbine.test
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Origin
import io.github.quasarapps.aquifer.aquifer
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.shadows.ShadowNetwork
import org.robolectric.shadows.ShadowNetworkCapabilities
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35])
class ConnectivityFlowTest {

    private val context: Context = ApplicationProvider.getApplicationContext()
    private val manager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    private val shadow = shadowOf(manager)

    /** Gives an existing shadow network internet capability, so the snapshot reads "online". */
    private fun seedOnline(): Network {
        val network = manager.allNetworks.first()
        val capabilities = ShadowNetworkCapabilities.newInstance()
        shadowOf(capabilities).addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
        shadow.setNetworkCapabilities(network, capabilities)
        return network
    }

    /** Removes every shadow network, so the snapshot reads "offline". */
    private fun seedOffline() {
        manager.allNetworks.forEach { shadow.removeNetwork(it) }
    }

    @Test
    fun `restoring connectivity after loss emits`() = runTest {
        val network = seedOnline()

        context.connectivityRestoredFlow().test {
            runCurrent() // the callback is now registered
            val callback = shadow.networkCallbacks.single()

            callback.onAvailable(network) // the system reports existing networks first
            callback.onLost(network)
            callback.onAvailable(network)

            assertEquals(Unit, awaitItem())
        }
    }

    @Test
    fun `connectivity that is already present does not emit`() = runTest {
        val network = seedOnline()

        context.connectivityRestoredFlow().test {
            runCurrent()
            val callback = shadow.networkCallbacks.single()

            callback.onAvailable(network)
            runCurrent()
            expectNoEvents()
        }
    }

    @Test
    fun `starting offline then regaining connectivity emits`() = runTest {
        seedOffline()

        context.connectivityRestoredFlow().test {
            runCurrent()
            val callback = shadow.networkCallbacks.single()

            // No initial callbacks arrive on an offline device; the first availability is
            // a genuine offline-to-online transition.
            callback.onAvailable(ShadowNetwork.newInstance(1))
            assertEquals(Unit, awaitItem())

            // Exactly once: a second network while online is not a restoration.
            callback.onAvailable(ShadowNetwork.newInstance(2))
            runCurrent()
            expectNoEvents()
        }
    }

    @Test
    fun `switching networks without losing connectivity does not emit`() = runTest {
        seedOnline()

        context.connectivityRestoredFlow().test {
            runCurrent()
            val callback = shadow.networkCallbacks.single()
            val wifi = ShadowNetwork.newInstance(1)
            val cellular = ShadowNetwork.newInstance(2)

            callback.onAvailable(wifi)
            callback.onAvailable(cellular)
            callback.onLost(wifi) // cellular still up — never fully offline
            runCurrent()
            expectNoEvents()

            callback.onLost(cellular) // now offline
            callback.onAvailable(wifi) // restored
            assertEquals(Unit, awaitItem())
        }
    }

    @Test
    fun `each restoration emits exactly once`() = runTest {
        val network = seedOnline()

        context.connectivityRestoredFlow().test {
            runCurrent()
            val callback = shadow.networkCallbacks.single()

            callback.onAvailable(network)
            callback.onLost(network)
            callback.onAvailable(network)
            assertEquals(Unit, awaitItem())

            // A second network appearing while online is not a restoration.
            callback.onAvailable(ShadowNetwork.newInstance(2))
            runCurrent()
            expectNoEvents()
        }
    }

    @Test
    fun `the callback is unregistered when collection stops`() = runTest {
        context.connectivityRestoredFlow().test {
            runCurrent()
            assertEquals(1, shadow.networkCallbacks.size)
            cancelAndIgnoreRemainingEvents()
        }
        runCurrent()
        assertTrue(shadow.networkCallbacks.isEmpty(), "callback should be unregistered")
    }

    @Test
    fun `revalidateOnReconnect refreshes a stale active stream`() = runTest {
        val network = seedOnline()
        var now = 0L
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock { now }
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.revalidateOnReconnect(context)
        runCurrent() // the trigger flow is now collecting; the callback is registered
        val callback = shadow.networkCallbacks.single()
        store.put("k", 100)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            now += 10.minutes.inWholeMilliseconds // entry goes stale while "offline"
            callback.onLost(network)
            callback.onAvailable(network) // connectivity restored

            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())
        }
        assertEquals(1, calls)
    }
}
