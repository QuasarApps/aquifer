package io.github.quasarapps.aquifer.sample

import io.github.quasarapps.aquifer.Aquifer
import io.github.quasarapps.aquifer.AquiferEvents
import io.github.quasarapps.aquifer.DataState
import io.github.quasarapps.aquifer.Freshness
import io.github.quasarapps.aquifer.aquifer
import io.github.quasarapps.aquifer.persistence.jsonFileSourceOfTruth
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * A runnable tour of Aquifer: stale-while-revalidate streams, request deduplication, local
 * writes, retries against a flaky API, surviving a "process restart" via disk persistence,
 * and refresh-on-reconnect.
 *
 * Run it with: `./gradlew :sample:run`
 */
@Serializable
data class Article(val id: Int, val title: String, val revision: Int)

/** Fake backend: slow, and every third call fails to show off retries. */
class FlakyArticlesApi {
    private val calls = AtomicInteger()
    private val revisions = AtomicInteger()

    suspend fun fetchArticle(id: Int): Article {
        delay(150) // simulated network latency
        val call = calls.incrementAndGet()
        if (call % 3 == 0) throw IOException("simulated outage on call #$call")
        return Article(id, "Article #$id", revisions.incrementAndGet())
    }
}

/** Logs engine activity — in an app this would be Timber/analytics. */
class LoggingEvents : AquiferEvents<Int> {
    override fun onFetchStarted(key: Int) = log("event: fetch started for $key")
    override fun onFetchSucceeded(key: Int, duration: Duration) = log("event: fetch for $key took $duration")
    override fun onFetchRetried(key: Int, attempt: Int, error: Throwable, nextDelay: Duration) =
        log("event: attempt #$attempt for $key failed (${error.message}); retrying in $nextDelay")

    override fun onFetchFailed(key: Int, error: Throwable, attempts: Int) =
        log("event: fetch for $key failed for good after $attempts attempt(s)")
}

fun main(): Unit = runBlocking {
    val cacheDir = Path("build/sample-cache")
    val api = FlakyArticlesApi()
    val reconnected = MutableSharedFlow<Unit>()

    fun newProcess(scope: CoroutineScope): Aquifer<Int, Article> = aquifer {
        scope(scope)
        fetcher { id -> api.fetchArticle(id) }
        freshness { timeToLive = 1.seconds }
        retry {
            maxAttempts = 3
            initialDelay = 100.milliseconds
        }
        persistence(jsonFileSourceOfTruth(cacheDir))
        events(LoggingEvents())
    }.also { it.revalidateOn(reconnected) }

    banner("1. Cold start: stream renders Loading, then the fetched article")
    val firstProcess = newProcess(this)
    firstProcess.deleteCachesForRepeatableDemo()
    val ui = launch {
        firstProcess.stream(1).collect { state -> log("ui:    ${state.render()}") }
    }
    delay(600)

    banner("2. Stale-while-revalidate: after the TTL, cached data shows instantly while refreshing")
    delay(1.seconds) // let the entry go stale
    log("ui:    (user reopens the screen)")
    val stale = firstProcess.get(1, Freshness.StaleWhileRevalidate)
    log("get -> \"${stale.title}\" rev=${stale.revision} (served stale immediately; refresh runs in background)")
    delay(600)

    banner("3. Local writes broadcast to every observer")
    firstProcess.put(1, Article(1, "Article #1 (edited offline)", revision = -1))
    delay(200)

    banner("4. 'Process death': a brand-new store serves the last data from disk, no network")
    ui.cancelAndJoin()
    firstProcess.close()
    val secondProcess = newProcess(this)
    val restored = secondProcess.get(1)
    log("get -> \"${restored.title}\" rev=${restored.revision} - straight from disk")

    banner("5. Reconnect: stale active streams refresh; every 3rd API call fails, so watch a retry")
    val ui2 = launch {
        secondProcess.stream(1).collect { state -> log("ui:    ${state.render()}") }
    }
    delay(1200) // entry goes stale while "offline"
    log("       (connectivity returns)")
    reconnected.emit(Unit)
    delay(900)

    ui2.cancelAndJoin()
    secondProcess.close()
    banner("Done. Cache files live in $cacheDir (cleared at startup so every run is identical).")
}

private suspend fun Aquifer<Int, Article>.deleteCachesForRepeatableDemo() = invalidateAll()

private fun DataState<Article>.render(): String = when (this) {
    is DataState.Loading -> "[loading] showing: ${value?.title ?: "nothing yet"}"
    is DataState.Content -> "[content] ${value.title} rev=${value.revision} (${origin}${if (isStale) ", stale" else ""})"
    is DataState.Failure -> "[failure] ${error.message} - still showing: ${value?.title ?: "nothing"}"
}

private fun banner(text: String) {
    println()
    println("-".repeat(72))
    println(text)
    println("-".repeat(72))
}

private fun log(message: String) = println("  $message")
