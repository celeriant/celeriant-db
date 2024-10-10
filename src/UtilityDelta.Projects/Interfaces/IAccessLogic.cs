using System.Threading;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IAccessLogic
    {
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