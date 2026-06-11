package io.github.quasarapps.aquifer.compose

import androidx.compose.runtime.Composable
import androidx.compose.runtime.State
import androidx.compose.runtime.remember
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import kotlinx.coroutines.flow.Flow
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Observes the data for [key] as Compose [State], collecting lifecycle-aware: collection
 * runs only while [lifecycleOwner] is at least [minActiveState] (so backgrounded screens
 * stop consuming the stream) and resumes on return — pairing naturally with
 * [Aquifer.revalidateOnAppForeground]-style refreshes.
 *
 * ```
 * @Composable
 * fun UserScreen(users: Aquifer<UserId, User>, id: UserId) {
 *     val state by users.collectAsState(id)
 *
 *     state.value?.let { UserProfile(it) }          // keep content visible while refreshing
 *     if (state.isLoading) RefreshIndicator()
 *     (state as? DataState.Failure)?.let { RefreshFailedSnackbar(it.error) }
 * }
 * ```
 *
 * The underlying stream is [remembered][rememberStream] per `(aquifer, key, freshness)`, so
 * recompositions don't restart it. Before the first emission the state is
 * `DataState.Loading(null)`.
 */
@Composable
public fun <K : Any, V : Any> Aquifer<K, V>.collectAsState(
    key: K,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
    lifecycleOwner: LifecycleOwner = LocalLifecycleOwner.current,
    minActiveState: Lifecycle.State = Lifecycle.State.STARTED,
    context: CoroutineContext = EmptyCoroutineContext,
): State<DataState<V>> = rememberStream(key, freshness).collectAsStateWithLifecycle(
    initialValue = DataState.Loading<V>(null),
    lifecycle = lifecycleOwner.lifecycle,
    minActiveState = minActiveState,
    context = context,
)

/**
 * Returns [Aquifer.stream] for [key], remembered across recompositions keyed on
 * `(aquifer, key, freshness)` — the building block for [collectAsState], exposed for cases
 * that need the raw [Flow] (custom operators, `produceState`, snapshotting in effects).
 */
@Composable
public fun <K : Any, V : Any> Aquifer<K, V>.rememberStream(
    key: K,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
): Flow<DataState<V>> = remember(this, key, freshness) { stream(key, freshness) }
