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
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.serializer
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.GeneralSecurityException
import java.security.MessageDigest
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readBytes

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
 *   fields to your model doesn't invalidate existing cache files. Removing or retyping a
 *   non-optional field is a breaking change — supply [schemaVersion] and [migrate] to upgrade
 *   old entries in place (see *Schema migration* below) instead of wiping the cache directory.
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
 * ### Schema migration
 *
 * By default a value whose stored JSON no longer matches [V] — a removed or retyped field —
 * is undecodable, so [read] heals the slot and the entry refetches. To upgrade old entries in
 * place instead, stamp writes with a [schemaVersion] and supply a [migrate] that rewrites an
 * older value's JSON to the current shape:
 *
 * ```
 * JsonFileSourceOfTruth(
 *     directory = dir,
 *     valueSerializer = User.serializer(),   // v2: { id, firstName, lastName }
 *     schemaVersion = 2,
 *     migrate = { fromVersion, value ->
 *         when (fromVersion) {
 *             // v0/v1 stored a single "name"; split it into first/last.
 *             0, 1 -> buildJsonObject {
 *                 val obj = value.jsonObject
 *                 put("id", obj.getValue("id"))
 *                 val parts = obj.getValue("name").jsonPrimitive.content.split(" ", limit = 2)
 *                 put("firstName", parts.first())
 *                 put("lastName", parts.getOrElse(1) { "" })
 *             }
 *             else -> null   // older version this build no longer understands: drop it
 *         }
 *     },
 * )
 * ```
 *
 * Migration runs lazily on [read] — the entry is rewritten in the new format the next time it
 * is written — and is called only for entries stored *below* the current [schemaVersion],
 * receiving that stored version so one callback can fan out across several. Returning `null`
 * drops the entry (healed away, then refetched), as does an entry stored *above*
 * [schemaVersion] (an app downgrade, whose shape this build can't know). A migrated tree that
 * still fails to decode is treated as corrupt and healed. A version-0 store (the default)
 * writes no version field and migrates nothing — byte-for-byte the pre-migration on-disk
 * format.
 *
 * ### Encryption at rest
 *
 * Pass a [cipher] to transform each entry's serialized bytes before they are written and after
 * they are read, so sensitive values aren't stored as plaintext JSON. The seam depends on
 * nothing beyond the JDK; back it with Google Tink's `Aead` (and the Android Keystore) through a
 * thin [ValueCipher] adapter. The on-disk bytes — and the [maxBytes] budget — are the ciphertext,
 * so a nonce/tag is accounted for at its real size, and a [ValueCipher.decrypt] that rejects the
 * bytes (wrong key, tampered or truncated file) heals the slot like any other corrupt entry.
 * Each entry's key is passed to the cipher as authenticated associated data, so a ciphertext
 * only decrypts under its own key — a blob copied or swapped to a different key's file on disk
 * is rejected (and healed), not served as that key's value. Encryption composes with bounding,
 * conditional fetching (validators are stored in the encrypted envelope), and
 * [schemaVersion]/[migrate] (migration sees the decrypted tree).
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
 * @param schemaVersion version stamped into every written entry. An entry read back at a lower
 *   version is passed to [migrate]; at a higher version it is dropped. `0` (the default) writes
 *   no version field, preserving the pre-migration on-disk format. Must be non-negative.
 * @param migrate upgrades an older entry's value JSON to the current [schemaVersion] shape,
 *   given the stored `fromVersion` and the raw value tree; returns the current-shape tree, or
 *   `null` to drop the entry (it refetches). Called only when `fromVersion < schemaVersion`.
 * @param cipher optional reversible transform applied to each entry's serialized bytes before
 *   they are written and after they are read — typically encryption. `null` (the default)
 *   stores plaintext JSON. The entry's key is passed as authenticated associated data, binding
 *   each ciphertext to its key. A [ValueCipher.decrypt] that throws is treated as an unreadable
 *   entry (healed and refetched), like any other corrupt file. See [ValueCipher].
 */
// Cohesive store (each helper is one locked or lock-free step of an I/O path) configured by
// defaulted knobs — a config object for two optional caps would be ceremony.
@Suppress("TooManyFunctions", "LongParameterList")
public class JsonFileSourceOfTruth<K : Any, V : Any>(
    private val directory: Path,
    private val valueSerializer: KSerializer<V>,
    private val keyEncoder: (K) -> String = { it.toString() },
    private val json: Json = DEFAULT_JSON,
    private val ioContext: CoroutineContext = Dispatchers.IO,
    private val maxEntries: Int? = null,
    private val maxBytes: Long? = null,
    private val schemaVersion: Int = 0,
    private val migrate: (fromVersion: Int, value: JsonElement) -> JsonElement? = { _, _ -> null },
    private val cipher: ValueCipher? = null,
) : SourceOfTruth<K, V> {

    init {
        require(maxEntries == null || maxEntries > 0) { "maxEntries must be positive, was $maxEntries" }
        require(maxBytes == null || maxBytes > 0) { "maxBytes must be positive, was $maxBytes" }
        require(schemaVersion >= 0) { "schemaVersion must be non-negative, was $schemaVersion" }
    }

    private val storedSerializer = Stored.serializer(valueSerializer)

    // Reads decode the value as a raw JSON tree only when a migration is actually needed (the
    // common path binds straight to [V]); this raw serializer is that migration path's first step.
    private val rawSerializer = Stored.serializer(JsonElement.serializer())

    // A lenient view used only to read an envelope's schemaVersion without binding its value;
    // ignoreUnknownKeys lets it skip the value/validator/writtenAtMillis fields.
    private val versionProbeJson = Json(json) { ignoreUnknownKeys = true }
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
            val raw = file.readBytes()
            val text = (cipher?.decrypt(raw, aadFor(key)) ?: raw).decodeToString()
            decodeStored(text) ?: return@withContext heal(file)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (_: SerializationException) {
            // Undecodable content: heal the slot so the next write recovers it.
            heal(file)
        } catch (_: IllegalArgumentException) {
            heal(file)
        } catch (_: GeneralSecurityException) {
            // The cipher rejected the bytes (wrong key, tampered or truncated file): the entry
            // is unrecoverable, so heal the slot exactly like any other corrupt file.
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
        val encoded = json.encodeToString(
            storedSerializer,
            Stored(entry.writtenAtMillis, entry.value, entry.validator, schemaVersion),
        )
        val plaintext = encoded.encodeToByteArray()
        // The on-disk bytes (and the byte budget) are the ciphertext: a cipher that pads or
        // adds a nonce/tag is accounted for at its real size. The entry's key is passed as
        // authenticated associated data, binding the ciphertext to its file (see [ValueCipher]).
        val bytes = cipher?.encrypt(plaintext, aadFor(key)) ?: plaintext
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

    /**
     * Decodes a stored entry from its (already decrypted) JSON [text], upgrading it to the
     * current [schemaVersion] when needed. Returns `null` when the entry must be dropped — a
     * version newer than this build can read (an app downgrade), or a [migrate] that declined —
     * so the caller heals the slot; a migrated tree that no longer binds to [V] throws and is
     * healed by [read]'s catch, like any other corrupt file.
     *
     * The common case — an entry already at [schemaVersion], including the default version 0 —
     * skips the raw [JsonElement] tree entirely: a cheap version probe, then a direct bind to
     * [V]. Only a true version mismatch pays for the tree plus [migrate] round-trip. (The probe
     * is needed because binding [V] first can't tell a current entry from an older one whose JSON
     * happens to decode into defaulted fields — that would silently skip migration.)
     */
    private fun decodeStored(text: String): PersistedEntry<V>? {
        val storedVersion = probeSchemaVersion(text)
        return when {
            storedVersion == schemaVersion -> {
                val stored = json.decodeFromString(storedSerializer, text)
                PersistedEntry(stored.value, stored.writtenAtMillis, stored.validator)
            }
            storedVersion > schemaVersion -> null // an app downgrade: this build can't know the shape
            else -> {
                val stored = json.decodeFromString(rawSerializer, text)
                val element = migrate(storedVersion, stored.value) ?: return null
                val value = json.decodeFromJsonElement(valueSerializer, element)
                PersistedEntry(value, stored.writtenAtMillis, stored.validator)
            }
        }
    }

    /**
     * Reads just the `schemaVersion` field of a stored envelope without binding its value — the
     * gate that lets the current-version read path skip the raw tree. A missing field (a
     * version-0 store) decodes as 0.
     */
    private fun probeSchemaVersion(text: String): Int =
        versionProbeJson.decodeFromString(VersionProbe.serializer(), text).schemaVersion

    /** Authenticated context bound to each ciphertext: the entry's encoded key (see [ValueCipher]). */
    private fun aadFor(key: K): ByteArray = keyEncoder(key).encodeToByteArray()

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
        // Defaulted for forward compatibility: pre-validator cache files decode as null.
        val validator: String? = null,
        // Defaulted so pre-migration cache files decode as version 0; with encodeDefaults off
        // a version-0 store writes no version field, so opting out is byte-for-byte the old format.
        val schemaVersion: Int = 0,
    )

    /** Probe envelope: just enough to read [Stored.schemaVersion] without binding the value. */
    @Serializable
    private class VersionProbe(val schemaVersion: Int = 0)

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
@Suppress("LongParameterList") // defaulted knobs; a config object for optional params would be ceremony
public inline fun <K : Any, reified V : Any> jsonFileSourceOfTruth(
    directory: Path,
    json: Json = JsonFileSourceOfTruth.DEFAULT_JSON,
    noinline keyEncoder: (K) -> String = { it.toString() },
    maxEntries: Int? = null,
    maxBytes: Long? = null,
    schemaVersion: Int = 0,
    noinline migrate: (fromVersion: Int, value: JsonElement) -> JsonElement? = { _, _ -> null },
    cipher: ValueCipher? = null,
): SourceOfTruth<K, V> = JsonFileSourceOfTruth(
    directory = directory,
    valueSerializer = json.serializersModule.serializer(),
    keyEncoder = keyEncoder,
    json = json,
    maxEntries = maxEntries,
    maxBytes = maxBytes,
    schemaVersion = schemaVersion,
    migrate = migrate,
    cipher = cipher,
)
