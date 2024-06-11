using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IAccessLogic
    {
        DtoShare CreateShareLink(string pi, string publicKey, string nonce, string sign, bool isOwner, bool singleUse, string? description, long expiresOn, bool readOnly);
    }
}