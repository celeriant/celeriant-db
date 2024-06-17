using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IShareKeyCache
    {
        /// <summary>
        /// Create a new share key, log it to the event stream and add it to the active cache.
        /// </summary>
        DtoShare CreateShareLink(string pi, string currentUserHash, bool isOwner, bool singleUse, string? description, long expiresOn, bool readOnly, CancellationToken cancellationToken);

        /// <summary>
        /// If the request presents with a sharekey, check if there is a match for that project in the event log. The request is cached.
        /// </summary>
        DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string shareKeyHash, CancellationToken cancellationToken);

        /// <summary>
        /// Disables the share key - could be a single use key or a user is manually de-activating the share key
        /// </summary>
        bool MarkShareKeyAsUsed(string projectId, string shareKeyHash, CancellationToken cancellationToken);
    }
}
