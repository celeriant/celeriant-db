using System.Threading;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IShareKeyCache
    {
        DtoShare CreateShareLink(string pi, string currentUserHash, bool isOwner, bool singleUse, string? description, long expiresOn, bool readOnly, CancellationToken cancellationToken);

        DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string shareKeyHash, CancellationToken cancellationToken);

        bool MarkShareKeyAsUsed(string projectId, string shareKeyHash, CancellationToken cancellationToken);
    }
}
