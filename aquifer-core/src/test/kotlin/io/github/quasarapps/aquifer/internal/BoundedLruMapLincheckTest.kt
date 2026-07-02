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
 * Lincheck linearizability check of [BoundedLruMap] (the structure backing the bounded negative
 * cache): all access is guarded by one monitor, so concurrent `get`/`put`/`remove`/`clear` must be
 * linearizable to some sequential order. A large `maxEntries` keeps eviction out of this check, so
 * the sequential specification is a plain map that Lincheck derives from sequential execution.
 *
 * Tagged `lincheck` so it runs only in the dedicated `lincheckTest` task, not `check`/`build`.
 */
@Tag("lincheck")
@Param(name = "key", gen = IntGen::class, conf = "1:3")
class BoundedLruMapLincheckTest {

    private val map = BoundedLruMap<Int, Int>(maxEntries = 10)

    @Operation
    fun put(@Param(name = "key") key: Int, value: Int) = map.put(key, value)

    @Operation
    fun get(@Param(name = "key") key: Int): Int? = map.get(key)

    @Operation
    fun remove(@Param(name = "key") key: Int) = map.remove(key)

    @Operation
    fun clear() = map.clear()

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
