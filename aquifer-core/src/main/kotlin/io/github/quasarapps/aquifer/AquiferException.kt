package io.github.quasarapps.aquifer

/** Base type for exceptions raised by Aquifer itself (as opposed to errors thrown by fetchers). */
public open class AquiferException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

/**
 * Thrown by [Aquifer.get] with [Freshness.CacheOnly] — and carried by [DataState.Failure] on
 * [Freshness.CacheOnly] streams — when no value is cached for the requested key.
 */
public class CacheMissException(key: Any) : AquiferException("No cached value for key '$key'")

/**
 * The failure attributed to a key that a [batch fetcher][AquiferBuilder.batchFetcher] omitted
 * from its result map — the backend returned no value for it. It fails only that key (its
 * awaiting [Aquifer.getAll] entry is dropped, its stream sees [DataState.Failure]); the other
 * keys in the same batch are unaffected.
 */
public class BatchKeyMissingException(key: Any) :
    AquiferException("Batch fetcher returned no value for key '$key'")
