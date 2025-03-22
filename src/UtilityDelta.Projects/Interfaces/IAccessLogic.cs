using System.Threading;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IAccessLogic
    {
        Task<bool> PullFromCloudIfNotPresentLocally(string projectId);
        DtoAccessInfo IsProjectExistAndHasAccess(
            string projectId,
            bool createProjectIfNotExists,
            string? shareKey,
            string publicKey,
            string nonce,
            string sign,
            CancellationToken cancellationToken,
            long? edOverride = null);
    }
}