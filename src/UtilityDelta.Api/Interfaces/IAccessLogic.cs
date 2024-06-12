using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IAccessLogic
    {
        (ProjectAccess projectAccess, string currentUserHash) IsProjectExistAndHasAccess(
            string projectId,
            bool createProjectIfNotExists,
            string? shareKey,
            string publicKey,
            string nonce,
            string sign);

        DtoShare CreateShareLink(string pi, string currentUserHash, bool isOwner, bool singleUse, string? description, long expiresOn, bool readOnly);
    }
}