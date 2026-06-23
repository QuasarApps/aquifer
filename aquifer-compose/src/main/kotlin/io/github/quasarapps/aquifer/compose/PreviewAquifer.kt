package io.github.quasarapps.aquifer.compose

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.CacheMissException
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.Origin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlin.time.Duration

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
 * for seeded keys and [DataState.Empty] for missing ones — previews never fetch, so absence
 * is an affirmative empty state, exactly as in a real cache-only stream (handy for
 * previewing empty layouts), and [Aquifer.streamMany] combines those per-key streams into one
 * map; [Aquifer.put]/[Aquifer.invalidate] update the seeded map and are reflected live in
 * active streams (interactive previews); [Aquifer.get] and [Aquifer.fresh] return a seeded
 * value or throw [CacheMissException], while [Aquifer.getAll] returns the seeded subset (never
 * throwing, as the real `getAll` does); [Aquifer.prefetch]/[Aquifer.prefetchAll], revalidation
 * hooks, and [Aquifer.close] are no-ops. Not suitable for production use.
 */
public fun <K : Any, V : Any> previewAquifer(vararg entries: Pair<K, V>): Aquifer<K, V> =
    PreviewAquifer(entries.toMap())

// A faithful Aquifer implementation: one override per interface member is inherent.
@Suppress("TooManyFunctions")
private class PreviewAquifer<K : Any, V : Any>(seed: Map<K, V>) : Aquifer<K, V> {

    private val snapshots = MutableStateFlow(seed)

    override fun stream(key: K, freshness: Freshness, maxAge: Duration?): Flow<DataState<V>> {
        requireValidMaxAge(maxAge)
        return snapshots
            .map { it[key] }
            .distinctUntilChanged()
            .map { value ->
                if (value != null) {
                    DataState.Content(value, Origin.MEMORY, isStale = false)
                } else {
                    DataState.Empty
                }
            }
    }

    override fun streamMany(keys: Set<K>, freshness: Freshness): Flow<Map<K, DataState<V>>> {
        if (keys.isEmpty()) return flowOf(emptyMap())
        val ordered = keys.toList()
        return combine(ordered.map { key -> stream(key, freshness) }) { states ->
            buildMap(ordered.size) { ordered.forEachIndexed { index, key -> put(key, states[index]) } }
        }
    }

    override suspend fun get(key: K, freshness: Freshness, maxAge: Duration?): V {
        requireValidMaxAge(maxAge)
        return snapshots.value[key] ?: throw CacheMissException(key)
    }

    /** Previews have no clock or TTL, so `maxAge` has no effect — but the contract is enforced. */
    private fun requireValidMaxAge(maxAge: Duration?) {
        require(maxAge == null || maxAge.isPositive()) {
            "maxAge must be positive, was $maxAge"
        }
    }

    override suspend fun fresh(key: K): V = get(key)

    override fun prefetch(key: K, freshness: Freshness) = Unit // previews never fetch

    override fun prefetchAll(keys: Set<K>, freshness: Freshness) = Unit // previews never fetch

    override suspend fun getAll(keys: Set<K>, freshness: Freshness): Map<K, V> {
        val seeded = snapshots.value
        return buildMap(keys.size) {
            for (key in keys) seeded[key]?.let { put(key, it) } // seeded subset; never fetches
        }
    }

    override suspend fun put(key: K, value: V) {
        snapshots.update { it + (key to value) }
    }

    override suspend fun putAll(entries: Map<K, V>) {
        snapshots.update { it + entries }
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
