using System.Security.Cryptography;

public class AesGcmEncryption
{
    private const int IvLength = 12; // Standard IV length for AES-GCM

    // Generate a random Initialization Vector (IV)
    public static byte[] CreateIV()
    {
        byte[] iv = new byte[IvLength];
        using (var rng = RandomNumberGenerator.Create())
        {
            rng.GetBytes(iv);
        }
        return iv;
    }

    private const int ENCRYPTION_KEY_BITS = 128; // Assuming 128 bits based on A128GCM

    public static AesGcm ImportKeyJwkSymmetric(string key)
    {
        // Decode the base64url encoded key
        byte[] keyBytes = Base64UrlDecode(key);

        // Ensure the key length matches the expected bit length
        if (keyBytes.Length * 8 != ENCRYPTION_KEY_BITS)
        {
            throw new ArgumentException($"Key length must be {ENCRYPTION_KEY_BITS} bits.");
        }

        return CreateAesGcm(keyBytes);
    }

    public static AesGcm CreateAesGcm(byte[] key)
    {
        return new AesGcm(key);
    }

    private static byte[] Base64UrlDecode(string input)
    {
        string padded = input.Length % 4 == 0 ? input : input + "====".Substring(input.Length % 4);
        string base64 = padded.Replace("-", "+").Replace("_", "/");
        return Convert.FromBase64String(base64);
    }

    //public static async Task<(byte[] encryptedData, byte[] iv)> EncryptSymmetricAsync(SymmetricAlgorithm key, byte[] unencryptedData)
    //{
    //    using var aes = Aes.Create();
    //    aes.Key = key.Key;
    //    aes.GenerateIV(); // Create a new IV
    //    var iv = aes.IV;

    //    using var encryptor = aes.CreateEncryptor(aes.Key, iv);
    //    using var ms = new MemoryStream();
    //    using var cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write);

    //    await cs.WriteAsync(unencryptedData, 0, unencryptedData.Length);
    //    await cs.FlushFinalBlockAsync();

    //    return (ms.ToArray(), iv);
    //}
    public static byte[] EncryptSymmetric(byte[] iv, AesGcm key, byte[] plaintext, byte[] associatedData = null)
    {
        const int tagSize = 16; // 128 bits for AES-GCM

        if (iv.Length != 12) // AES-GCM typically uses a 12-byte (96-bit) IV
        {
            throw new ArgumentException("IV must be 12 bytes long for AES-GCM.", nameof(iv));
        }

        // Prepare buffers for the ciphertext and tag
        byte[] ciphertext = new byte[plaintext.Length];
        byte[] tag = new byte[tagSize];

        // Encrypt the data
        key.Encrypt(iv, plaintext, ciphertext, tag, associatedData);

        // Combine ciphertext and tag
        byte[] encryptedData = new byte[ciphertext.Length + tagSize];
        Array.Copy(ciphertext, 0, encryptedData, 0, ciphertext.Length);
        Array.Copy(tag, 0, encryptedData, ciphertext.Length, tagSize);

        return encryptedData;
    }

    public static byte[] DecryptSymmetric(byte[] iv, AesGcm key, byte[] encryptedData, byte[] associatedData = null)
    {
        // The tag size must match what was used for encryption (typically 16 bytes / 128 bits for AES-GCM)
        const int tagSize = 16;

        if (encryptedData.Length < tagSize)
        {
            throw new ArgumentException("Encrypted data is too short to contain a valid authentication tag.");
        }

        // Split the encrypted data into ciphertext and authentication tag
        int ciphertextLength = encryptedData.Length - tagSize;
        byte[] ciphertext = new byte[ciphertextLength];
        byte[] tag = new byte[tagSize];
        Array.Copy(encryptedData, 0, ciphertext, 0, ciphertextLength);
        Array.Copy(encryptedData, ciphertextLength, tag, 0, tagSize);

        // Prepare a buffer for the decrypted data
        byte[] decryptedData = new byte[ciphertextLength];

        try
        {
            // Decrypt the data
            key.Decrypt(iv, ciphertext, tag, decryptedData, associatedData);
            return decryptedData;
        }
        catch (CryptographicException)
        {
            // This exception is thrown if the tag doesn't match (i.e., the data has been tampered with)
            throw new CryptographicException("Decryption failed. The data may have been tampered with.");
        }
    }

}
