package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.SourceOfTruth
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText

/**
 * A [SourceOfTruth] that stores each entry as a JSON file inside [directory].
 *
 * ```
 * val users = aquifer<UserId, User> {
 *     fetcher { api.fetchUser(it) }
 *     persistence(
 *         JsonFileSourceOfTruth(
 *             directory = context.filesDir.resolve("aquifer/users").toPath(),
 *             valueSerializer = User.serializer(),
 *         )
 *     )
 * }
 * ```
 *
 * ### Storage model
 *
 * - One file per key, named by the SHA-256 of the encoded key — arbitrary key strings are
 *   filesystem-safe, and the chance of two keys colliding is cryptographically negligible.
 * - [directory] must be dedicated to this store: [deleteAll] removes every `.json` file in it.
 * - Writes go to a temp file that is fsynced and then atomically moved into place, so readers
 *   never observe a torn file and a crash mid-write leaves the previous entry intact. (The
 *   *new* entry's durability across a power loss immediately after the rename is best-effort
 *   — the directory entry itself is not fsynced.) On the rare filesystem without atomic
 *   moves a plain replace is used instead, which weakens the crash guarantee to best-effort.
 * - Temp files orphaned by a crash mid-write are deleted the first time the store touches
 *   the filesystem, bounded or not.
 * - On Android this module requires API 26+ (it is built on `java.nio.file`), or
 *   `coreLibraryDesugaring` with the NIO-enabled desugaring artifact for lower API levels.
 * - Undecodable files are treated as absent: [read] returns `null` and deletes the corrupt
 *   file so the slot heals on the next write. A read that fails with a (possibly transient)
 *   I/O error is also reported as absent, but the file is kept — it may read fine next time.
 *   Schema evolution is tolerated by default: [DEFAULT_JSON] ignores unknown keys, so adding
 *   fields to your model doesn't invalidate existing cache files (removing non-optional
 *   fields does — bump the directory name when making breaking model changes).
 *
 * ### Bounded operation
 *
 * By default the store grows without limit. Pass [maxEntries] and/or [maxBytes] to bound it:
 * after every write, least-recently-used entries are evicted until both budgets hold, and a
 * store found over budget the first time it is touched (say, after limits were lowered
 * between releases) is trimmed the same way. The caps are absolute — even a brand-new entry
 * is evicted when it alone exceeds [maxBytes], so such a value simply never persists (memory
 * cache and fetcher still serve it; the next process start refetches). Each eviction pass is
 * best-effort: an entry whose file cannot be deleted right now ends the pass with the store
 * still over budget, and the next write retries it.
 *
 * Recency is tracked exactly within a process — reads and writes both count as use — and
 * approximated across restarts by file modification time, which amounts to last-write order
 * because reads deliberately don't touch files. Eviction bookkeeping assumes this instance
 * is the directory's only live writer; the dedicated-directory requirement above already
 * implies that.
 *
 * ### Concurrency
 *
 * Safe for concurrent use from multiple coroutines: concurrent writes to a key resolve to one
 * of the written values (atomic replace, never a mix), and reads see either the old or the
 * new entry. All I/O runs on [ioContext], `Dispatchers.IO` by default.
 *
 * @param directory dedicated directory for this store's files; created on first write.
 * @param valueSerializer serializer for the value type, e.g. `User.serializer()`.
 * @param keyEncoder stable string form of keys used for file naming; defaults to `toString()`.
 *   Two keys must encode equally if and only if they are the same logical key.
 * @param json JSON configuration; defaults to [DEFAULT_JSON] (`ignoreUnknownKeys = true`).
 * @param ioContext context for file I/O; replace in tests for full determinism if desired.
 * @param maxEntries optional cap on the number of stored entries, enforced by LRU eviction.
 *   `null` (the default) means unbounded. Must be positive.
 * @param maxBytes optional cap on the total size of the stored files in bytes, enforced by
 *   LRU eviction. `null` (the default) means unbounded. Must be positive.
 */
// Cohesive store (each helper is one locked or lock-free step of an I/O path) configured by
// defaulted knobs — a config object for two optional caps would be ceremony.
@Suppress("TooManyFunctions", "LongParameterList")
public class JsonFileSourceOfTruth<K : Any, V : Any>(
    private val directory: Path,
    valueSerializer: KSerializer<V>,
    private val keyEncoder: (K) -> String = { it.toString() },
    private val json: Json = DEFAULT_JSON,
    private val ioContext: CoroutineContext = Dispatchers.IO,
    private val maxEntries: Int? = null,
    private val maxBytes: Long? = null,
) : SourceOfTruth<K, V> {

    init {
        require(maxEntries == null || maxEntries > 0) { "maxEntries must be positive, was $maxEntries" }
        require(maxBytes == null || maxBytes > 0) { "maxBytes must be positive, was $maxBytes" }
    }

    private val storedSerializer = Stored.serializer(valueSerializer)
    private val bounded = maxEntries != null || maxBytes != null

    /** Guards [index], [totalBytes], one-time housekeeping, and (when [bounded]) renames. */
    private val housekeeping = Mutex()

    @Volatile
    private var housekeepingDone = false

    /** File name → size in bytes, least-recently-used first. Guarded by [housekeeping]. */
    private val index = LinkedHashMap<String, Long>()
    private var totalBytes = 0L

    override suspend fun read(key: K): PersistedEntry<V>? = withContext(ioContext) {
        val file = fileFor(key)
        try {
            ensureHousekeeping()
            if (!file.exists()) {
                forgetIfMissing(file)
                return@withContext null
            }
            // Recency is bumped when the read observes the file, not after the decode, so a
            // slow read can't lose its place to an eviction pass racing it.
            recordAccess(file)
            val stored = json.decodeFromString(storedSerializer, file.readText())
            PersistedEntry(stored.value, stored.writtenAtMillis)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (_: SerializationException) {
            // Undecodable content: heal the slot so the next write recovers it.
            heal(file)
        } catch (_: IllegalArgumentException) {
            heal(file)
        } catch (_: IOException) {
            // Possibly transient (permissions, mounts, pressure): report absent but keep the
            // file — deleting here could destroy a perfectly good entry.
            null
        }
    }

    override suspend fun write(key: K, entry: PersistedEntry<V>): Unit = withContext(ioContext) {
        ensureHousekeeping()
        directory.createDirectories()
        val encoded = json.encodeToString(storedSerializer, Stored(entry.writtenAtMillis, entry.value))
        val bytes = encoded.encodeToByteArray()
        val target = fileFor(key)
        val temp = directory.resolve("${target.fileName}.${UUID.randomUUID()}$TEMP_SUFFIX")
        try {
            // Force the temp file's contents to disk before the rename: journaling
            // filesystems may otherwise commit the rename before the data, and a power loss
            // would replace the previous entry with a torn or empty file.
            FileChannel.open(temp, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW).use { channel ->
                val buffer = ByteBuffer.wrap(bytes)
                // A single write() may consume only part of the buffer; drain it fully.
                while (buffer.hasRemaining()) {
                    channel.write(buffer)
                }
                channel.force(true)
            }
            if (bounded) {
                // Rename and accounting commit together, so the budget math can never drift
                // from which file content actually won a same-key race.
                housekeeping.withLock {
                    moveIntoPlace(temp, target)
                    record(target, bytes.size.toLong())
                    evictWhileOverBudget()
                }
            } else {
                moveIntoPlace(temp, target)
            }
        } finally {
            temp.deleteIfExists()
        }
    }

    override suspend fun delete(key: K): Unit = withContext(ioContext) {
        ensureHousekeeping()
        val file = fileFor(key)
        if (bounded) {
            housekeeping.withLock {
                file.deleteIfExists()
                dropAccounting(file)
            }
        } else {
            file.deleteIfExists()
        }
    }

    override suspend fun deleteAll(): Unit = withContext(ioContext) {
        ensureHousekeeping()
        if (bounded) {
            housekeeping.withLock {
                deleteAllFiles()
                index.clear()
                totalBytes = 0
            }
        } else {
            deleteAllFiles()
        }
    }

    private fun deleteAllFiles() {
        if (!directory.exists()) return
        directory.listDirectoryEntries().forEach { file ->
            if (file.extension == FILE_EXTENSION || file.fileName.toString().endsWith(TEMP_SUFFIX)) {
                file.deleteIfExists()
            }
        }
    }

    /**
     * One-time, before any other filesystem access from this instance: delete temp files
     * orphaned by a crash mid-write (none of ours can exist yet), and, when [bounded], seed
     * the LRU index from the directory — eldest modification time first — then trim to
     * budget in case the directory outgrew newly lowered limits between runs.
     */
    private suspend fun ensureHousekeeping() {
        if (housekeepingDone) return
        housekeeping.withLock {
            if (housekeepingDone || !directory.exists()) {
                housekeepingDone = true
                return
            }
            val entries = directory.listDirectoryEntries()
            entries.filter { it.fileName.toString().endsWith(TEMP_SUFFIX) }
                .forEach { temp -> runCatching { temp.deleteIfExists() } }
            if (bounded) {
                index.clear()
                totalBytes = 0
                entries.filter { it.extension == FILE_EXTENSION }
                    .map { file ->
                        // A file whose metadata can't be read still occupies the directory:
                        // index it with zero size and eldest priority so it stays counted
                        // and evictable instead of invisible to the budget.
                        Triple(
                            file.fileName.toString(),
                            runCatching { Files.size(file) }.getOrDefault(0L),
                            runCatching { Files.getLastModifiedTime(file).toMillis() }.getOrDefault(0L),
                        )
                    }
                    .sortedBy { (_, _, modifiedAt) -> modifiedAt }
                    .forEach { (name, size, _) ->
                        index[name] = size
                        totalBytes += size
                    }
                evictWhileOverBudget()
            }
            housekeepingDone = true
        }
    }

    private fun moveIntoPlace(temp: Path, target: Path) {
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            // Documented fallback: non-atomic replace on filesystems without atomic moves.
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    /** Records [target] as most recently used at [size] bytes. Caller holds [housekeeping]. */
    private fun record(target: Path, size: Long) {
        val name = target.fileName.toString()
        index.remove(name)?.let { previous -> totalBytes -= previous }
        index[name] = size
        totalBytes += size
    }

    /**
     * Deletes least-recently-used entries until both budgets hold. Caller holds
     * [housekeeping]. A delete that fails ends the pass instead of spinning on a stuck
     * file; the next write retries it.
     */
    private fun evictWhileOverBudget() {
        while (entriesOverBudget() || bytesOverBudget()) {
            val eldest = index.entries.iterator().next()
            try {
                directory.resolve(eldest.key).deleteIfExists()
            } catch (_: IOException) {
                return
            }
            totalBytes -= eldest.value
            index.remove(eldest.key)
        }
    }

    private fun entriesOverBudget(): Boolean = maxEntries != null && index.size > maxEntries

    private fun bytesOverBudget(): Boolean = maxBytes != null && totalBytes > maxBytes

    /** Bumps [file] to most-recently-used; a no-op for unbounded stores or unknown files. */
    private suspend fun recordAccess(file: Path) {
        if (!bounded) return
        housekeeping.withLock {
            val name = file.fileName.toString()
            val size = index.remove(name) ?: return
            index[name] = size
        }
    }

    /**
     * Drops [file]'s accounting only if it is still absent — re-checked under the lock, so
     * a same-key write that lands between the caller's miss and this call keeps the fresh
     * record it just made.
     */
    private suspend fun forgetIfMissing(file: Path) {
        if (!bounded) return
        housekeeping.withLock {
            if (!file.exists()) dropAccounting(file)
        }
    }

    /** Removes [file]'s accounting entry. Caller holds [housekeeping]. */
    private fun dropAccounting(file: Path) {
        index.remove(file.fileName.toString())?.let { size -> totalBytes -= size }
    }

    /**
     * Deletes an unreadable file so the slot heals; the entry is reported as absent. The
     * delete and the accounting drop commit together — like [delete] — so a racing
     * same-key write is either fully before this (its file is the one healed away) or
     * fully after (its record survives untouched), never half-observed.
     */
    private suspend fun heal(file: Path): PersistedEntry<V>? {
        if (bounded) {
            housekeeping.withLock {
                // Accounting is dropped only when the file is verifiably gone (deleted now,
                // or already absent). A failed delete keeps the entry budgeted — the bytes
                // are still on disk — matching the eviction loop's failure mode.
                runCatching { file.deleteIfExists() }.onSuccess { dropAccounting(file) }
            }
        } else {
            runCatching { file.deleteIfExists() }
        }
        return null
    }

    private fun fileFor(key: K): Path {
        val digest = MessageDigest.getInstance("SHA-256").digest(keyEncoder(key).encodeToByteArray())
        val name = digest.joinToString("") { byte -> "%02x".format(byte.toInt() and BYTE_MASK) }
        return directory.resolve("$name.$FILE_EXTENSION")
    }

    /** On-disk envelope pairing the value with its original write time. */
    @Serializable
    private class Stored<V>(
        val writtenAtMillis: Long,
        val value: V,
    )

    public companion object {
        /** Default JSON configuration: unknown keys are ignored so added model fields don't invalidate caches. */
        public val DEFAULT_JSON: Json = Json { ignoreUnknownKeys = true }

        private const val FILE_EXTENSION = "json"
        private const val TEMP_SUFFIX = ".tmp"
        private const val BYTE_MASK = 0xff
    }
}

/**
 * Creates a [JsonFileSourceOfTruth] with the serializer for [V] resolved from [json]'s
 * serializers module:
 *
 * ```
 * val store = aquifer<UserId, User> {
 *     fetcher { api.fetchUser(it) }
 *     persistence(jsonFileSourceOfTruth(cacheDir.toPath().resolve("users")))
 * }
 * ```
 */
public inline fun <K : Any, reified V : Any> jsonFileSourceOfTruth(
    directory: Path,
    json: Json = JsonFileSourceOfTruth.DEFAULT_JSON,
    noinline keyEncoder: (K) -> String = { it.toString() },
    maxEntries: Int? = null,
    maxBytes: Long? = null,
): SourceOfTruth<K, V> = JsonFileSourceOfTruth(
    directory = directory,
    valueSerializer = json.serializersModule.serializer(),
    keyEncoder = keyEncoder,
    json = json,
    maxEntries = maxEntries,
    maxBytes = maxBytes,
)
