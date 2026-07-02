package io.github.quasarapps.aquifer.internal

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.junit.jupiter.api.Tag
import kotlin.test.Test

/**
 * Lincheck linearizability check of [MemoryCache]: every public operation is guarded by the same
 * monitor, so concurrent `get`/`put`/`remove`/`clear` must be linearizable to some sequential
 * order. A large `maxEntries` keeps eviction out of this check, so the sequential specification is
 * a plain map — Lincheck derives it by running the operations sequentially.
 *
 * Tagged `lincheck` so it runs only in the dedicated `lincheckTest` task, not the fast unit-test
 * task (`check`/`build`): model-checking explores many interleavings and takes minutes.
 */
@Tag("lincheck")
@Param(name = "key", gen = IntGen::class, conf = "1:3")
class MemoryCacheLincheckTest {

    private val cache = MemoryCache<Int, Int>(maxEntries = 10)

    @Operation
    fun put(@Param(name = "key") key: Int, value: Int) {
        cache.put(key, MemoryCache.Entry(value, writtenAtMillis = 0L, sequence = 0L))
    }

    @Operation
    fun get(@Param(name = "key") key: Int): Int? = cache.get(key)?.value

    @Operation
    fun remove(@Param(name = "key") key: Int) = cache.remove(key)

    @Operation
    fun clear() = cache.clear()

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
