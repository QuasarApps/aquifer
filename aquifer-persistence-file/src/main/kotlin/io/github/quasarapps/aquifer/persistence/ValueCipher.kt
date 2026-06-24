package io.github.quasarapps.aquifer.persistence

/**
 * A reversible transform applied to each entry's serialized bytes on the way to and from disk,
 * so [JsonFileSourceOfTruth] can store values encrypted (or otherwise obfuscated) at rest.
 * [decrypt] must invert [encrypt] for bytes this cipher produced.
 *
 * The seam is key-management-agnostic and depends on nothing beyond the JDK, so a production
 * cipher — e.g. Google Tink's `Aead`, backed by the Android Keystore — plugs in with a thin
 * adapter:
 *
 * ```
 * class TinkValueCipher(private val aead: Aead) : ValueCipher {
 *     override fun encrypt(plaintext: ByteArray): ByteArray = aead.encrypt(plaintext, null)
 *     override fun decrypt(ciphertext: ByteArray): ByteArray = aead.decrypt(ciphertext, null)
 * }
 * ```
 *
 * Implementations should be safe to call concurrently from multiple coroutines: the store
 * ciphers reads and writes for different keys in parallel.
 */
public interface ValueCipher {

    /** Transforms [plaintext] into the bytes to store on disk. */
    public fun encrypt(plaintext: ByteArray): ByteArray

    /**
     * Recovers the plaintext from [ciphertext] previously produced by [encrypt]. Should throw
     * [java.security.GeneralSecurityException] when the bytes can't be decrypted or authenticated
     * (a wrong key, or a tampered or truncated file): [JsonFileSourceOfTruth] then treats the
     * entry as corrupt — healing the slot and refetching — instead of letting the failure reach
     * the caller.
     */
    public fun decrypt(ciphertext: ByteArray): ByteArray
}
