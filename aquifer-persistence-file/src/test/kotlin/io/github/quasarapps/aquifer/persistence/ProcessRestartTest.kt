package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.aquifer
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable
private data class Article(val id: String, val title: String)

/** End-to-end: data fetched in one "process" is served from disk by the next. */
class ProcessRestartTest {

    @TempDir
    lateinit var dir: Path

    @Test
    fun `data fetched before a restart is served from disk after it`() = runTest {
        var networkCalls = 0
        fun newProcess() = aquifer<String, Article> {
            scope(backgroundScope)
            fetcher { id ->
                networkCalls++
                Article(id, "Fetched title")
            }
            persistence(jsonFileSourceOfTruth(dir.resolve("articles")))
        }

        // First app session: cache miss, fetch, write-through to disk.
        val firstSession = newProcess()
        assertEquals(Article("a1", "Fetched title"), firstSession.get("a1"))
        assertEquals(1, networkCalls)
        firstSession.close()

        // Second app session ("after process death"): served from disk, no network.
        val secondSession = newProcess()
        assertEquals(Article("a1", "Fetched title"), secondSession.get("a1"))
        assertEquals(1, networkCalls)
        secondSession.close()
    }
}
