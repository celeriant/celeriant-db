using NanoidDotNet;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Services
{
    public class AccessLogic(IFileHandlesManager fileHandlesManager, ICrypto crypto, IUserAccessCache userAccessCache, IShareKeyCache shareKeyCache, IWriteAndBackup writeAndBackup) : IAccessLogic
    {
        public async Task<bool> PullFromCloudIfNotPresentLocally(string projectId)
        {
            // Check if project exists locally
            if (fileHandlesManager.Exists(projectId))
            {
                // Project already exists locally, no need to pull from cloud
                return true;
            }

            // Attempt to read the project from cloud storage
            bool success = await writeAndBackup.ReadFromCloud(projectId);

            // Return whether the operation was successful
            return success && fileHandlesManager.Exists(projectId);
        }

        public DtoAccessInfo IsProjectExistAndHasAccess(
            string projectId,
            bool createProjectIfNotExists,
            string? shareKey,
            string publicKey,
            string nonce,
            string sign,
            CancellationToken cancellationToken,
            long? edOverride = null)
        {
            //Validate the user's public key and get their identity (SHA-256 hash)
            crypto.ValidateWithPublicKey(publicKey, nonce, sign);
            var currentUserHash = publicKey.CalculateHash();

            //The orignal sharekey is not store in event stream, only its hash
            shareKey = shareKey?.CalculateHash();

            if (!fileHandlesManager.Exists(projectId))
            {
                //No record of this project, either return not exists or auto-create it for the user and give them owner access
                if (!createProjectIfNotExists) return new DtoAccessInfo(ProjectAccess.NotExists, currentUserHash);

                userAccessCache.UpdateAccess(projectId, null, currentUserHash, AccessLevel.Owner, null, "Project creator", false, null, edOverride, cancellationToken);

                return new DtoAccessInfo(ProjectAccess.OwnerAccess, currentUserHash);
            }

            var currentAccessLevel = userAccessCache.GetCurrentAccess(projectId, currentUserHash, cancellationToken);

            //Get share key data if a key is provided, but only operate on active share keys
            var shareKeyData = shareKey == null ? null : shareKeyCache.GetShareKeyDataIfStillValid(projectId, shareKey, cancellationToken);

            if (shareKeyData != null && shareKeyData.createdBy != currentUserHash && shareKeyData.isSingleUse)
            {
                //Users who created the share key can't expire their own key (in case they click it first)
                //Otherwise if this share key is single use mark it as expired
                if (shareKeyCache.MarkShareKeyAsUsed(projectId, null, shareKey!, cancellationToken) == null)
                {
                    //Could fail due to share key already used (thread contention)
                    shareKeyData = null;
                }
            }

            if (shareKeyData != null && currentAccessLevel.IncreasesAccessLevel(shareKeyData.accessLevel))
            {
                //the current user gets an increase in their access level with the given share key
                userAccessCache.UpdateAccess(projectId, null, currentUserHash, shareKeyData.accessLevel, shareKeyData.iv, shareKeyData.description, false, shareKey, null, cancellationToken);
                return new DtoAccessInfo(shareKeyData.accessLevel.ToProjectAccess(), currentUserHash);
            }

            //No current access and no current sharekey
            if (!currentAccessLevel.HasValue)
            {
                return new DtoAccessInfo(ProjectAccess.NoAccess, currentUserHash);
            }

            return new DtoAccessInfo(currentAccessLevel.Value.ToProjectAccess(), currentUserHash);
        }
    }
}
