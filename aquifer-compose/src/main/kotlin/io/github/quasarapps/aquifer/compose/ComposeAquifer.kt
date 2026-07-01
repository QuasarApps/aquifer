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
import kotlin.time.Duration

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
 * The underlying stream is [remembered][rememberStream] per `(aquifer, key, freshness,
 * maxAge)`, so recompositions don't restart it — but changing any of those, including
 * [maxAge], starts a fresh collection. Before the first emission the state is
 * `DataState.Loading(null)`.
 */
@Composable
@Suppress("LongParameterList") // Defaulted knobs mirroring collectAsStateWithLifecycle — idiomatic Compose shape.
public fun <K : Any, V : Any> Aquifer<K, V>.collectAsState(
    key: K,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
    maxAge: Duration? = null,
    lifecycleOwner: LifecycleOwner = LocalLifecycleOwner.current,
    minActiveState: Lifecycle.State = Lifecycle.State.STARTED,
    context: CoroutineContext = EmptyCoroutineContext,
): State<DataState<V>> = rememberStream(key, freshness, maxAge).collectAsStateWithLifecycle(
    initialValue = DataState.Loading<V>(null),
    lifecycle = lifecycleOwner.lifecycle,
    minActiveState = minActiveState,
    context = context,
)

/**
 * Returns [Aquifer.stream] for [key], remembered across recompositions keyed on
 * `(aquifer, key, freshness, maxAge)` — the building block for [collectAsState], exposed for
 * cases that need the raw [Flow] (custom operators, `produceState`, snapshotting in effects).
 */
@Composable
public fun <K : Any, V : Any> Aquifer<K, V>.rememberStream(
    key: K,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
    maxAge: Duration? = null,
): Flow<DataState<V>> = remember(this, key, freshness, maxAge) { stream(key, freshness, maxAge) }

/**
 * Observes every key in [keys] as one combined Compose [State], collecting lifecycle-aware
 * exactly like the single-key [collectAsState]: **one** collector for the whole set (not one
 * per item), so a list or grid screen holds a single lifecycle-scoped subscription instead of
 * a per-item collector that restarts as items scroll in and out. The member keys' initial
 * fetches are batched into one call, mirroring [Aquifer.streamMany].
 *
 * ```
 * @Composable
 * fun UsersRow(users: Aquifer<UserId, User>, ids: Set<UserId>) {
 *     val states by users.collectAsState(ids)
 *
 *     ids.forEach { id ->
 *         when (val state = states[id]) {
 *             is DataState.Content -> UserChip(state.value)
 *             null -> UserChipPlaceholder()          // not resolved yet (before the first emission)
 *             else -> UserChipLoading(state)
 *         }
 *     }
 * }
 * ```
 *
 * The underlying stream is [remembered][rememberStreamMany] per `(aquifer, keys, freshness)`, so
 * recompositions don't restart it — but changing the [keys] set (by value) or [freshness] starts
 * a fresh collection and a new batched initial fetch. Before the first emission the state is an
 * **empty map**; the first emission carries an entry for every member key — cached `Content`, or
 * `Loading` on a miss that will fetch (a `CacheOnly` miss is `Empty`). Unlike the single-key
 * overload there is no `maxAge` knob — [Aquifer.streamMany] does not take one.
 *
 * The result is one `State<Map>`: any single key's change recomposes every reader of the whole
 * map (Compose still skips unchanged leaf composables). That is the tradeoff of one combined
 * subscription versus a per-item collector — aimed at bounded lists/grids, not unbounded sets.
 */
@Composable
public fun <K : Any, V : Any> Aquifer<K, V>.collectAsState(
    keys: Set<K>,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
    lifecycleOwner: LifecycleOwner = LocalLifecycleOwner.current,
    minActiveState: Lifecycle.State = Lifecycle.State.STARTED,
    context: CoroutineContext = EmptyCoroutineContext,
): State<Map<K, DataState<V>>> = rememberStreamMany(keys, freshness).collectAsStateWithLifecycle(
    initialValue = emptyMap<K, DataState<V>>(),
    lifecycle = lifecycleOwner.lifecycle,
    minActiveState = minActiveState,
    context = context,
)

/**
 * Returns [Aquifer.streamMany] for [keys], remembered across recompositions keyed on
 * `(aquifer, keys, freshness)` — the building block for the multi-key [collectAsState], exposed
 * for cases that need the raw [Flow] (custom operators, `produceState`, snapshotting in effects).
 * The multi-key counterpart to [rememberStream]. Keyed on the [keys] set by value, so passing a
 * fresh set with the same contents across recompositions reuses the same stream — but pass an
 * immutable/stable set: mutating a `MutableSet` in place (same instance) won't restart the stream,
 * since `remember` compares it equal to itself.
 */
@Composable
public fun <K : Any, V : Any> Aquifer<K, V>.rememberStreamMany(
    keys: Set<K>,
    freshness: Freshness = Freshness.StaleWhileRevalidate,
): Flow<Map<K, DataState<V>>> = remember(this, keys, freshness) { streamMany(keys, freshness) }
