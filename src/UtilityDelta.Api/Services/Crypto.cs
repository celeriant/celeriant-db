using System.Security.Cryptography;
using UtilityDelta.Api.Exceptions;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class Crypto : ICrypto
    {
        private static double MAX_NONCE_TIME_MINUMTES = 2.0;

        public void ValidateWithPublicKey(
            string publicKey,
            string nonce,
            string sign)
        {
            ValidateNonce(nonce);
            ValidateSignature(publicKey, nonce, sign);
        }

        private static void ValidateSignature(string publicKey, string nonce, string sign)
        {
            try
            {
                var rsaPublicKey = RSA.Create();
                rsaPublicKey.ImportFromPem(publicKey);
                var nonceData = nonce.ToByteArray();
                var signData = Convert.FromBase64String(sign);
                var result = rsaPublicKey.VerifyData(nonceData, signData, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                if (!result) throw new ExceptionInvalidSignature();
            }
            catch
            {
                throw new ExceptionInvalidSignature();
            }
        }

        private static void ValidateNonce(string nonce)
        {
            try
            {
                var nonceDatetime = DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(nonce));
                var timeDiff = DateTimeOffset.UtcNow - nonceDatetime;
                if (timeDiff.TotalMinutes > MAX_NONCE_TIME_MINUMTES) throw new ExceptionInvalidNonce();
            }
            catch
            {
                throw new ExceptionInvalidNonce();
            }
        }
    }
}
