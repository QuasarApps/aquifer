package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
import io.github.quasarapps.aquifer.SourceOfTruth
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializable
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
 * - On Android this module requires API 26+ (it is built on `java.nio.file`), or
 *   `coreLibraryDesugaring` with the NIO-enabled desugaring artifact for lower API levels.
 * - Undecodable files are treated as absent: [read] returns `null` and deletes the corrupt
 *   file so the slot heals on the next write. A read that fails with a (possibly transient)
 *   I/O error is also reported as absent, but the file is kept — it may read fine next time.
 *   Schema evolution is tolerated by default: [DEFAULT_JSON] ignores unknown keys, so adding
 *   fields to your model doesn't invalidate existing cache files (removing non-optional
 *   fields does — bump the directory name when making breaking model changes).
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
 */
public class JsonFileSourceOfTruth<K : Any, V : Any>(
    private val directory: Path,
    valueSerializer: KSerializer<V>,
    private val keyEncoder: (K) -> String = { it.toString() },
    private val json: Json = DEFAULT_JSON,
    private val ioContext: CoroutineContext = Dispatchers.IO,
) : SourceOfTruth<K, V> {

    private val storedSerializer = Stored.serializer(valueSerializer)

    override suspend fun read(key: K): PersistedEntry<V>? = withContext(ioContext) {
        val file = fileFor(key)
        if (!file.exists()) return@withContext null
        try {
            val stored = json.decodeFromString(storedSerializer, file.readText())
            PersistedEntry(stored.value, stored.writtenAtMillis)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (corrupt: SerializationException) {
            heal(file)
        } catch (corrupt: IllegalArgumentException) {
            heal(file)
        } catch (transient: IOException) {
            // Possibly transient (permissions, mounts, pressure): report absent but keep the
            // file — deleting here could destroy a perfectly good entry.
            null
        }
    }

    override suspend fun write(key: K, entry: PersistedEntry<V>): Unit = withContext(ioContext) {
        directory.createDirectories()
        val encoded = json.encodeToString(storedSerializer, Stored(entry.writtenAtMillis, entry.value))
        val target = fileFor(key)
        val temp = directory.resolve("${target.fileName}.${UUID.randomUUID()}$TEMP_SUFFIX")
        try {
            // Force the temp file's contents to disk before the rename: journaling
            // filesystems may otherwise commit the rename before the data, and a power loss
            // would replace the previous entry with a torn or empty file.
            FileChannel.open(temp, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW).use { channel ->
                val buffer = ByteBuffer.wrap(encoded.encodeToByteArray())
                // A single write() may consume only part of the buffer; drain it fully.
                while (buffer.hasRemaining()) {
                    channel.write(buffer)
                }
                channel.force(true)
            }
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (unsupported: AtomicMoveNotSupportedException) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
            }
        } finally {
            temp.deleteIfExists()
        }
    }

    override suspend fun delete(key: K): Unit = withContext(ioContext) {
        fileFor(key).deleteIfExists()
    }

    override suspend fun deleteAll(): Unit = withContext(ioContext) {
        if (!directory.exists()) return@withContext
        directory.listDirectoryEntries().forEach { file ->
            if (file.extension == FILE_EXTENSION || file.fileName.toString().endsWith(TEMP_SUFFIX)) {
                file.deleteIfExists()
            }
        }
    }

    /** Deletes an unreadable file so the slot heals; the entry is reported as absent. */
    private fun heal(file: Path): PersistedEntry<V>? {
        runCatching { file.deleteIfExists() }
        return null
    }

    private fun fileFor(key: K): Path {
        val digest = MessageDigest.getInstance("SHA-256").digest(keyEncoder(key).encodeToByteArray())
        val name = digest.joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
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
): SourceOfTruth<K, V> = JsonFileSourceOfTruth(
    directory = directory,
    valueSerializer = json.serializersModule.serializer(),
    keyEncoder = keyEncoder,
    json = json,
)
