package io.github.quasarapps.aquifer

/** `true` while a fetch is in flight for the observed key; sugar for `this is DataState.Loading`. */
public val DataState<*>.isLoading: Boolean
    get() = this is DataState.Loading

/**
 * Returns the freshest value or throws: a [DataState.Failure] rethrows its [DataState.Failure.error],
 * and a state with no value yet (e.g. the initial `Loading(null)`) throws [NoSuchElementException].
 * Useful at boundaries that cannot render partial states.
 */
public fun <V : Any> DataState<V>.valueOrThrow(): V = when (this) {
    is DataState.Failure -> throw error
    else -> value ?: throw NoSuchElementException("No value available yet")
}

/**
 * Transforms the carried value while preserving the state's shape: `Loading` stays `Loading`,
 * `Content` keeps its [DataState.Content.origin] and staleness, `Failure` keeps its error —
 * each with [transform] applied to the value it carries (when one exists).
 *
 * ```
 * val names: DataState<String> = userState.map { it.displayName }
 * ```
 */
public inline fun <V : Any, R : Any> DataState<V>.map(transform: (V) -> R): DataState<R> = when (this) {
    is DataState.Loading -> DataState.Loading(value?.let(transform))
    is DataState.Content -> DataState.Content(transform(value), origin, isStale)
    is DataState.Failure -> DataState.Failure(error, value?.let(transform))
}

/** Runs [action] with the value when this is [DataState.Content]; returns this state for chaining. */
public inline fun <V : Any> DataState<V>.onContent(action: (V) -> Unit): DataState<V> {
    if (this is DataState.Content) action(value)
    return this
}

/** Runs [action] with the error when this is [DataState.Failure]; returns this state for chaining. */
public inline fun <V : Any> DataState<V>.onFailure(action: (Throwable) -> Unit): DataState<V> {
    if (this is DataState.Failure) action(error)
    return this
}
