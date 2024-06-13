using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IShareKeyCache
    {
        DtoShare CreateShareLink(string pi, string currentUserHash, bool isOwner, bool singleUse, string? description, long expiresOn, bool readOnly);

        DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string shareKeyHash);

        bool MarkShareKeyAsUsed(string projectId, string shareKeyHash);
    }
}
