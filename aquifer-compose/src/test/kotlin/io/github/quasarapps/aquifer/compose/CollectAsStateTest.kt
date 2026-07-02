package io.github.quasarapps.aquifer.compose

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.testing.TestLifecycleOwner
import app.cash.molecule.RecompositionMode
import app.cash.molecule.moleculeFlow
import app.cash.turbine.test
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Origin
import io.github.quasarapps.aquifer.aquifer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Before
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class CollectAsStateTest {

    // collectAsStateWithLifecycle's repeatOnLifecycle hops through Dispatchers.Main, which
    // has no platform implementation in JVM unit tests.
    @Before
    fun installMainDispatcher() = Dispatchers.setMain(UnconfinedTestDispatcher())

    @After
    fun resetMainDispatcher() = Dispatchers.resetMain()

    private fun owner(state: Lifecycle.State = Lifecycle.State.RESUMED) =
        TestLifecycleOwner(state, UnconfinedTestDispatcher())

    @Test
    fun `renders loading then the fetched content`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }
        val owner = owner()

        moleculeFlow(RecompositionMode.Immediate) {
            store.collectAsState("k", lifecycleOwner = owner).value
        }.test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
        }
    }

    @Test
    fun `collection waits for the lifecycle to reach the active state`() = runTest {
        var calls = 0
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher {
                calls++
                "fetched"
            }
        }
        val owner = owner(Lifecycle.State.CREATED) // below STARTED: stream must not start

        moleculeFlow(RecompositionMode.Immediate) {
            store.collectAsState("k", lifecycleOwner = owner).value
        }.test {
            assertEquals(DataState.Loading(null), awaitItem()) // just the initial value
            expectNoEvents()
            assertEquals(0, calls)

            owner.handleLifecycleEvent(Lifecycle.Event.ON_START)

            assertEquals(DataState.Content("fetched", Origin.FETCHER, isStale = false), awaitItem())
            assertEquals(1, calls)
        }
    }

    @Test
    fun `local writes reach the composition`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }
        store.put("k", "seeded")
        val owner = owner()

        moleculeFlow(RecompositionMode.Immediate) {
            store.collectAsState("k", lifecycleOwner = owner).value
        }.test {
            assertEquals(DataState.Loading(null), awaitItem())
            assertEquals(DataState.Content("seeded", Origin.MEMORY, isStale = false), awaitItem())

            store.put("k", "edited")

            assertEquals(DataState.Content("edited", Origin.LOCAL, isStale = false), awaitItem())
        }
    }

    @Test
    fun `rememberStream returns the same stream across recompositions`() = runTest {
        val store = aquifer<String, String> {
            scope(backgroundScope)
            fetcher { "fetched" }
        }
        val seen = mutableListOf<Flow<DataState<String>>>()
        var tick by mutableStateOf(0)

        moleculeFlow(RecompositionMode.Immediate) {
            seen += store.rememberStream("k")
            tick
        }.test {
            assertEquals(0, awaitItem())
            tick = 1
            assertEquals(1, awaitItem())
            assertSame(seen.first(), seen.last())
            assertEquals(2, seen.size)
        }
    }

    @Test
    fun `collectAsStateMany renders an empty map then the combined content`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }
        val owner = owner()

        moleculeFlow(RecompositionMode.Immediate) {
            store.collectAsStateMany(setOf("a", "bb"), lifecycleOwner = owner).value
        }.test {
            assertEquals(emptyMap<String, DataState<Int>>(), awaitItem()) // initial, before the first emission
            // streamMany's combine emits once every member has a state; skip intermediate Loading maps.
            var latest = awaitItem()
            while (latest.values.any { it !is DataState.Content }) latest = awaitItem()
            assertEquals(
                mapOf(
                    "a" to DataState.Content(1, Origin.FETCHER, isStale = false),
                    "bb" to DataState.Content(2, Origin.FETCHER, isStale = false),
                ),
                latest,
            )
        }
    }

    @Test
    fun `rememberStreamMany returns the same stream across recompositions for an equal key set`() = runTest {
        val store = aquifer<String, Int> {
            scope(backgroundScope)
            batchFetcher { keys -> keys.associateWith { it.length } }
        }
        val seen = mutableListOf<Flow<Map<String, DataState<Int>>>>()
        var tick by mutableStateOf(0)

        moleculeFlow(RecompositionMode.Immediate) {
            seen += store.rememberStreamMany(setOf("a", "bb")) // a fresh but equal set each pass
            tick
        }.test {
            assertEquals(0, awaitItem())
            tick = 1
            assertEquals(1, awaitItem())
            assertSame(seen.first(), seen.last()) // Set value-equality keying reuses the stream
            assertEquals(2, seen.size)
        }
    }
}
