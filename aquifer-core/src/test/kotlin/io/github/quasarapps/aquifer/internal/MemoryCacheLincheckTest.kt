package io.github.quasarapps.aquifer.internal

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import kotlin.test.Test

/**
 * Lincheck linearizability check of [MemoryCache]: every public operation is guarded by the same
 * monitor, so concurrent `get`/`put`/`remove`/`clear` must be linearizable to some sequential
 * order. A large `maxEntries` keeps eviction out of this first probe, so the sequential
 * specification is a plain map — Lincheck derives it from running the operations sequentially.
 */
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

    @Test
    fun stressTest() = StressOptions().check(this::class)

    @Test
    fun modelCheckingTest() = ModelCheckingOptions().check(this::class)
}
