package io.github.quasarapps.aquifer.internal

import io.github.quasarapps.aquifer.Origin
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The reconciliation matrix for a new stream collector's initial snapshot: hydrated disk
 * snapshots must survive pure LRU eviction but never survive invalidation, and origins must
 * be reported truthfully.
 */
class SnapshotReconciliationTest {

    private fun entry(sequence: Long) = MemoryCache.Entry("value", writtenAtMillis = 0L, sequence = sequence)

    @Test
    fun `memory hit matching the hydrated commit keeps the persistence origin`() {
        val preloaded = RealAquifer.Snapshot(entry(sequence = 7), Origin.PERSISTENCE)

        val resolved = RealAquifer.reconcileSnapshot(preloaded, entry(sequence = 7), epochUnchanged = true)

        assertEquals(preloaded, resolved)
    }

    @Test
    fun `a newer memory commit outranks the hydrated snapshot`() {
        val preloaded = RealAquifer.Snapshot(entry(sequence = 7), Origin.PERSISTENCE)
        val newer = entry(sequence = 8)

        val resolved = RealAquifer.reconcileSnapshot(preloaded, newer, epochUnchanged = true)

        assertEquals(RealAquifer.Snapshot(newer, Origin.MEMORY), resolved)
    }

    @Test
    fun `eviction between hydration and subscription keeps the snapshot`() {
        val preloaded = RealAquifer.Snapshot(entry(sequence = 7), Origin.PERSISTENCE)

        // Memory empty but the epoch never moved: the gap was LRU pressure, not invalidation.
        val resolved = RealAquifer.reconcileSnapshot(preloaded, inMemory = null, epochUnchanged = true)

        assertEquals(preloaded, resolved)
    }

    @Test
    fun `invalidation between hydration and subscription drops the snapshot`() {
        val preloaded = RealAquifer.Snapshot(entry(sequence = 7), Origin.PERSISTENCE)

        val resolved = RealAquifer.reconcileSnapshot(preloaded, inMemory = null, epochUnchanged = false)

        assertNull(resolved)
    }

    @Test
    fun `nothing hydrated and nothing in memory resolves to no snapshot`() {
        assertNull(RealAquifer.reconcileSnapshot<String>(null, inMemory = null, epochUnchanged = true))
    }
}
