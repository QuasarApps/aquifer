package io.github.quasarapps.aquifer.persistence

/**
 * A reversible transform applied to each entry's serialized bytes on the way to and from disk,
 * so [JsonFileSourceOfTruth] can store values encrypted (or otherwise obfuscated) at rest.
 * [decrypt] must invert [encrypt] for bytes this cipher produced under the same
 * [associatedData].
 *
 * [associatedData] is authenticated but **not** encrypted context that binds a ciphertext to
 * where it belongs: [JsonFileSourceOfTruth] passes the entry's encoded key, so a blob only
 * decrypts under its own key. An AEAD cipher (the expected implementation) must therefore fail
 * [decrypt] when the [associatedData] differs from what [encrypt] was given — which is how a
 * blob copied or swapped to a *different* key's file on disk is rejected rather than served as
 * that key's value.
 *
 * The seam is key-management-agnostic and depends on nothing beyond the JDK, so a production
 * cipher — e.g. Google Tink's `Aead`, backed by the Android Keystore — plugs in with a thin
 * adapter (its `encrypt`/`decrypt` already take associated data):
 *
 * ```
 * class TinkValueCipher(private val aead: Aead) : ValueCipher {
 *     override fun encrypt(plaintext: ByteArray, associatedData: ByteArray): ByteArray =
 *         aead.encrypt(plaintext, associatedData)
 *     override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray): ByteArray =
 *         aead.decrypt(ciphertext, associatedData)
 * }
 * ```
 *
 * Implementations should be safe to call concurrently from multiple coroutines: the store
 * ciphers reads and writes for different keys in parallel.
 */
public interface ValueCipher {

    /**
     * Transforms [plaintext] into the bytes to store on disk, authenticating (but not
     * encrypting) [associatedData] alongside them so [decrypt] can reject bytes presented with
     * different context.
     */
    public fun encrypt(plaintext: ByteArray, associatedData: ByteArray): ByteArray

    /**
     * Recovers the plaintext from [ciphertext] previously produced by [encrypt] under the same
     * [associatedData]. Should throw [java.security.GeneralSecurityException] when the bytes
     * can't be decrypted or authenticated — a wrong key, a tampered or truncated file, or
     * [associatedData] that doesn't match what [encrypt] was given (a relocated blob):
     * [JsonFileSourceOfTruth] then treats the entry as corrupt — healing the slot and
     * refetching — instead of letting the failure reach the caller.
     */
    public fun decrypt(ciphertext: ByteArray, associatedData: ByteArray): ByteArray
}
