package io.github.quasarapps.aquifer.android

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.testing.TestLifecycleOwner
import app.cash.turbine.test
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Origin
import io.github.quasarapps.aquifer.aquifer
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35])
class AppForegroundFlowTest {

    private fun owner(initialState: Lifecycle.State = Lifecycle.State.STARTED) =
        TestLifecycleOwner(initialState, UnconfinedTestDispatcher())

    @Test
    fun `an already-started lifecycle does not emit on subscription`() = runTest {
        val owner = owner()

        appForegroundedFlow(owner.lifecycle).test {
            runCurrent()
            expectNoEvents()
        }
    }

    @Test
    fun `subscribing while backgrounded emits on the next start`() = runTest {
        val owner = owner(initialState = Lifecycle.State.CREATED)

        appForegroundedFlow(owner.lifecycle).test {
            runCurrent()
            expectNoEvents()

            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)

            assertEquals(Unit, awaitItem())
        }
    }

    @Test
    fun `returning to the foreground emits`() = runTest {
        val owner = owner()

        appForegroundedFlow(owner.lifecycle).test {
            runCurrent()

            owner.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)

            assertEquals(Unit, awaitItem())
        }
    }

    @Test
    fun `each foreground return emits exactly once`() = runTest {
        val owner = owner()

        appForegroundedFlow(owner.lifecycle).test {
            runCurrent()

            owner.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)
            assertEquals(Unit, awaitItem())

            owner.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)
            assertEquals(Unit, awaitItem())
            expectNoEvents()
        }
    }

    @Test
    fun `the observer is removed when collection stops`() = runTest {
        val owner = owner()

        appForegroundedFlow(owner.lifecycle).test {
            runCurrent()
            assertEquals(1, owner.observerCount)
            cancelAndIgnoreRemainingEvents()
        }
        runCurrent()
        assertEquals(0, owner.observerCount)
    }

    @Test
    fun `revalidateOnAppForeground refreshes a stale active stream`() = runTest {
        val owner = owner()
        var now = 0L
        var calls = 0
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            clock { now }
            fetcher { ++calls }
            freshness { timeToLive = 1.minutes }
        }
        store.revalidateOnAppForeground(owner.lifecycle)
        runCurrent()
        store.put("k", 100)

        store.stream("k").test {
            assertEquals(DataState.Content(100, Origin.MEMORY, isStale = false), awaitItem())

            now += 10.minutes.inWholeMilliseconds // entry goes stale while backgrounded
            owner.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)

            assertEquals(DataState.Loading(100), awaitItem())
            assertEquals(DataState.Content(1, Origin.FETCHER, isStale = false), awaitItem())
        }
        assertEquals(1, calls)
    }
}
