using System.Security.Cryptography;
using System.Text;
using UtilityDelta.Api.Exceptions;
using UtilityDelta.Api.Services;

namespace UtilityDelta.Api.Tests
{
    [TestClass]
    public class TestCrypto
    {
        [TestMethod]
        public void TestMethodValid()
        {
            var assymetric = RSA.Create();
            var publicKeyPEM = assymetric.ExportRSAPublicKeyPem();
            var nonce = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            string nonceSigned = SignNonce(assymetric, nonce);

            var service = new Crypto();
            service.ValidateWithPublicKey(publicKeyPEM, nonce, nonceSigned);
        }

        [TestMethod]
        [ExpectedException(typeof(ExceptionInvalidNonce))]
        public void TestMethodOldNonce()
        {
            var assymetric = RSA.Create();
            var publicKeyPEM = assymetric.ExportRSAPublicKeyPem();
            var nonce = DateTimeOffset.UtcNow
                .Subtract(TimeSpan.FromMinutes(2.01))
                .ToUnixTimeSeconds().ToString();
            string nonceSigned = SignNonce(assymetric, nonce);

            var service = new Crypto();
            service.ValidateWithPublicKey(publicKeyPEM, nonce, nonceSigned);
        }

        [TestMethod]
        [ExpectedException(typeof(ExceptionInvalidSignature))]
        public void TestMethodFakeSignNonce()
        {
            var assymetric = RSA.Create();
            var publicKeyPEM = assymetric.ExportRSAPublicKeyPem();
            var nonce = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            
            var wrongAssymetric = RSA.Create();
            string nonceSigned = SignNonce(wrongAssymetric, nonce);

            var service = new Crypto();
            service.ValidateWithPublicKey(publicKeyPEM, nonce, nonceSigned);
        }

        private static string SignNonce(RSA assymetric, string nonce) => 
            Convert.ToBase64String(assymetric.SignData(Encoding.UTF8.GetBytes(nonce), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
    }
}