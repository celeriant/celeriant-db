using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IUserAccessCache
    {
        AccessLevel? GetCurrentAccess(string projectId, string currentUserHash);
        ProjectEventItem? UpdateAccess(string projectId, string? currentUserHash, string forUserId, AccessLevel? accessLevel, string? description, bool allowDowngrade, string? shareKey);
    }
}
