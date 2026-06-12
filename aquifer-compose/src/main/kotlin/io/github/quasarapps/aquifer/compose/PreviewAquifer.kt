package io.github.quasarapps.aquifer.compose

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update

/**
 * An [Aquifer] for `@Preview` composables and UI tests: serves only the seeded entries and
 * **never fetches** — no coroutine scope, no network, no disk.
 *
 * ```
 * @Preview
 * @Composable
 * fun UserScreenPreview() {
 *     UserScreen(users = previewAquifer("u1" to User("u1", "Ada")), id = "u1")
 * }
 * ```
 *
 * Semantics, scoped to what previews need: [Aquifer.stream] emits `Content(value, MEMORY)`
 * for seeded keys and `Failure(CacheMissException)` for missing ones (handy for previewing
 * error states); [Aquifer.put]/[Aquifer.invalidate] update the seeded map and are reflected
 * live in active streams (interactive previews); [Aquifer.get] and [Aquifer.fresh] return
 * seeded values or throw [CacheMissException]; revalidation hooks and [Aquifer.close] are
 * no-ops. Not suitable for production use.
 */
public fun <K : Any, V : Any> previewAquifer(vararg entries: Pair<K, V>): Aquifer<K, V> =
    PreviewAquifer(entries.toMap())

private class PreviewAquifer<K : Any, V : Any>(seed: Map<K, V>) : Aquifer<K, V> {

    private val snapshots = MutableStateFlow(seed)

    override fun stream(key: K, freshness: Freshness): Flow<DataState<V>> =
        snapshots
            .map { it[key] }
            .distinctUntilChanged()
            .map { value ->
                if (value != null) {
                    DataState.Content(value, Origin.MEMORY, isStale = false)
                } else {
                    DataState.Failure(CacheMissException(key))
                }
            }

    override suspend fun get(key: K, freshness: Freshness): V =
        snapshots.value[key] ?: throw CacheMissException(key)

    override suspend fun fresh(key: K): V = get(key)

    override suspend fun put(key: K, value: V) {
        snapshots.update { it + (key to value) }
    }

    override suspend fun invalidate(key: K) {
        snapshots.update { it - key }
    }

    override suspend fun invalidateAll() {
        snapshots.value = emptyMap()
    }

    override suspend fun revalidateActive() = Unit

    override fun revalidateOn(trigger: Flow<*>) = Unit

    override fun close() = Unit
}
