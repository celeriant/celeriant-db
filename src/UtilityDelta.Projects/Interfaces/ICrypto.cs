namespace UtilityDelta.Projects.Interfaces
{
    public interface ICrypto
    {
        void ValidateWithPublicKey(string publicKey, string nonce, string sign);
    }
}
