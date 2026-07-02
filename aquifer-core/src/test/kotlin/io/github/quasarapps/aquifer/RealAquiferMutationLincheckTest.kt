package io.github.quasarapps.aquifer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.junit.jupiter.api.Tag
import kotlin.test.Test

/**
 * Lincheck check of the real engine's **fetch-free** mutation region. The operation set —
 * `put`/`invalidate`/`invalidateAll` (under the `commitGuard` coroutine `Mutex`) plus
 * `get(CacheOnly)`/`snapshot` (lock-free memory reads) — runs entirely in the calling coroutine and
 * never reaches the fetch transport (`scope.async` in `refreshWith`), so Lincheck's scheduler fully
 * controls every interleaving. With no persistence and a large memory cap (no eviction), the visible
 * state is linearizable to a plain map, which Lincheck derives by running the operations sequentially.
 *
 * ### What this does and does not cover
 *
 * This is deliberately narrow:
 * - **Canary:** it proves `suspend` `@Operation`s and a coroutine `Mutex` are schedulable under
 *   Lincheck 2.39 on the real engine — the prerequisite for the extracted fencing models to come.
 * - **Guard:** it pins atomicity/ordering of the `commitGuard`-serialized mutation region.
 *
 * It does **not** cover the epoch-fencing guarantee. With no fetch and no persistence, no executed
 * path reads the epoch, so a fence/epoch regression (e.g. deleting a `fence()` call) is invisible
 * here — only a fetch commit writes memory without moving the epoch, and the fetch transport is on
 * the injected scope, outside Lincheck's control. Fencing (#42) and residual hydration (#13/#20)
 * stay in the hand-written interleaving tests, plus the extracted `EpochFence`/`SingleFlightRegistry`
 * Lincheck models planned as the follow-up (ROADMAP 0.5).
 *
 * Tagged `lincheck` so it runs only in the dedicated `lincheckTest` task, not `check`/`build`.
 */
@Tag("lincheck")
@Param(name = "key", gen = IntGen::class, conf = "1:2")
class RealAquiferMutationLincheckTest {

    // The scope is never used: the fetch-free operation set never triggers a fetch, so the
    // fetcher below never runs and no work is ever launched on this scope.
    private val store: Aquifer<Int, Int> = aquifer {
        scope(CoroutineScope(Dispatchers.Unconfined))
        fetcher { error("fetch must not happen in the fetch-free Lincheck operation set") }
    }

    @Operation
    suspend fun put(@Param(name = "key") key: Int, value: Int) = store.put(key, value)

    @Operation
    suspend fun invalidate(@Param(name = "key") key: Int) = store.invalidate(key)

    @Operation
    suspend fun invalidateAll() = store.invalidateAll()

    @Operation
    suspend fun getCacheOnly(@Param(name = "key") key: Int): Int? =
        try {
            store.get(key, Freshness.CacheOnly)
        } catch (miss: CacheMissException) {
            null
        }

    @Operation
    fun snapshot(): Set<Int> = store.snapshot()

    @Test
    fun stressTest() = StressOptions()
        .iterations(30)
        .threads(3)
        .actorsPerThread(3)
        .check(this::class)

    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .iterations(30)
        .threads(2)
        .actorsPerThread(3)
        .actorsBefore(2)
        .actorsAfter(1)
        .check(this::class)
}
