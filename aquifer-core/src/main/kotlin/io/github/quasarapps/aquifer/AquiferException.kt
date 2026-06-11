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
