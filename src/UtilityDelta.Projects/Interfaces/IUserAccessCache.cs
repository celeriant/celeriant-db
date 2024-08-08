using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IUserAccessCache
    {
        AccessLevel? GetCurrentAccess(string projectId, string currentUserHash, CancellationToken cancellationToken);
        ProjectEventItem? UpdateAccess(string projectId, string? currentUserHash, string forUserId, AccessLevel? accessLevel, string? iv, string? description, bool allowDowngrade, string? shareKey, CancellationToken cancellationToken);
    }
}
