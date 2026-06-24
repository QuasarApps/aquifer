package io.github.quasarapps.aquifer.persistence

import io.github.quasarapps.aquifer.PersistedEntry
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.security.GeneralSecurityException
import kotlin.io.path.extension
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readBytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Serializable
private data class Secret(val id: String, val token: String)

/** A trivial reversible cipher for tests: XOR is its own inverse. Not real cryptography. */
private class XorCipher(private val mask: Int) : ValueCipher {
    override fun encrypt(plaintext: ByteArray): ByteArray =
        ByteArray(plaintext.size) { (plaintext[it].toInt() xor mask).toByte() }

    override fun decrypt(ciphertext: ByteArray): ByteArray = encrypt(ciphertext)
}

/** A cipher whose [decrypt] always rejects the bytes, modelling a wrong key or tampering. */
private object RejectingCipher : ValueCipher {
    override fun encrypt(plaintext: ByteArray): ByteArray = plaintext
    override fun decrypt(ciphertext: ByteArray): ByteArray = throw GeneralSecurityException("bad ciphertext")
}

class EncryptionTest {

    @TempDir
    lateinit var dir: Path

    private fun store(cipher: ValueCipher) =
        JsonFileSourceOfTruth<String, Secret>(dir.resolve("s"), Secret.serializer(), cipher = cipher)

    private fun jsonFiles() = dir.resolve("s").listDirectoryEntries().filter { it.extension == "json" }

    @Test
    fun `a ciphered entry reads back identically`() = runTest {
        val store = store(XorCipher(0x5A))
        val entry = PersistedEntry(Secret("u1", "s3cr3t-token"), writtenAtMillis = 10)

        store.write("u1", entry)

        assertEquals(entry, store.read("u1"))
    }

    @Test
    fun `values are stored ciphered, not as plaintext`() = runTest {
        store(XorCipher(0x5A)).write("u1", PersistedEntry(Secret("u1", "s3cr3t-token"), writtenAtMillis = 10))

        val onDisk = jsonFiles().single().readBytes().decodeToString()
        assertFalse(onDisk.contains("s3cr3t-token"), "plaintext leaked to disk: $onDisk")
        assertFalse(onDisk.contains("\"id\""), "JSON structure leaked to disk: $onDisk")
    }

    @Test
    fun `a ciphered entry survives a fresh store instance with the same cipher`() = runTest {
        store(XorCipher(0x5A)).write("u1", PersistedEntry(Secret("u1", "s3cr3t-token"), writtenAtMillis = 10))

        // A new instance models a process restart: same directory, same cipher.
        assertEquals(Secret("u1", "s3cr3t-token"), store(XorCipher(0x5A)).read("u1")?.value)
    }

    @Test
    fun `the wrong key yields undecodable bytes and heals the slot`() = runTest {
        store(XorCipher(0x5A)).write("u1", PersistedEntry(Secret("u1", "s3cr3t-token"), writtenAtMillis = 10))

        // A different mask decrypts to garbage that fails to parse as JSON.
        assertNull(store(XorCipher(0x33)).read("u1"))
        assertTrue(jsonFiles().isEmpty(), "an undecodable entry should be healed away")
    }

    @Test
    fun `a decrypt that throws heals the slot`() = runTest {
        // Write plaintext, then read with a cipher that rejects everything.
        JsonFileSourceOfTruth<String, Secret>(dir.resolve("s"), Secret.serializer())
            .write("u1", PersistedEntry(Secret("u1", "s3cr3t-token"), writtenAtMillis = 10))

        assertNull(store(RejectingCipher).read("u1"))
        assertTrue(jsonFiles().isEmpty(), "a rejected entry should be healed away")
    }

    @Test
    fun `byte bounding counts the ciphertext size`() = runTest {
        // A cipher that doubles the byte size, so the on-disk entry is twice its plaintext.
        val doubling = object : ValueCipher {
            override fun encrypt(plaintext: ByteArray): ByteArray = plaintext + plaintext
            override fun decrypt(ciphertext: ByteArray): ByteArray = ciphertext.copyOf(ciphertext.size / 2)
        }
        val entry = PersistedEntry(Secret("u1", "x"), writtenAtMillis = 1)

        // Measure the plaintext size for this exact entry, then clear the slot.
        val plain = JsonFileSourceOfTruth<String, Secret>(dir.resolve("s"), Secret.serializer())
        plain.write("u1", entry)
        val plaintextSize = jsonFiles().single().readBytes().size.toLong()
        plain.delete("u1")

        // A budget that fits the plaintext but not the 2x ciphertext: the entry alone is over
        // budget, so it is not retained — proving the budget counts the ciphertext, not the value.
        val bounded = JsonFileSourceOfTruth<String, Secret>(
            dir.resolve("s"),
            Secret.serializer(),
            cipher = doubling,
            maxBytes = plaintextSize,
        )
        bounded.write("u1", entry)

        assertNull(bounded.read("u1"))
        assertTrue(jsonFiles().isEmpty(), "ciphertext exceeding maxBytes should not be retained")
    }
}
