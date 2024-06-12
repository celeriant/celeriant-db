using Microsoft.AspNetCore.Mvc;
using NanoidDotNet;
using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class AccessLogic(ICrypto crypto, IWriteEvents writeEvents, IReadEvents readEvents) : IAccessLogic
    {
        public DtoShare CreateShareLink(
            string pi,
            string currentUserHash,
            bool isOwner,
            bool singleUse,
            string? description,
            long expiresOn,
            bool readOnly)
        {
            var code = Nanoid.Generate();
            var hashedCode = code.CalculateHash();

            var tp = singleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink;
            var accessLevel = isOwner ? AccessLevel.Owner : readOnly ? AccessLevel.Viewer : AccessLevel.Contributor;

            var shareEvent = new ProjectEventItem(0, currentUserHash, 0, null, tp,
                description, accessLevel.ToString(), hashedCode, expiresOn);

            var (lastServerId, eventDate) = writeEvents.Write([shareEvent], currentUserHash, pi);

            shareEvent = new ProjectEventItem(lastServerId, currentUserHash, eventDate, null, tp,
                description, accessLevel.ToString(), hashedCode, expiresOn);

            return new DtoShare(code, shareEvent);
        }

        public (ProjectAccess projectAccess, string currentUserHash) IsProjectExistAndHasAccess(
            string projectId,
            bool createProjectIfNotExists,
            string? shareKey,
            string publicKey,
            string nonce,
            string sign)
        {
            //The orignal sharekey is not store in event stream, only its hash
            shareKey = shareKey?.CalculateHash();

            crypto.ValidateWithPublicKey(publicKey, nonce, sign);
            var currentUserHash = publicKey.CalculateHash();

            if (!readEvents.Exists(projectId))
            {
                if (!createProjectIfNotExists) return (ProjectAccess.NotExists, currentUserHash);

                ProvideAccess(projectId, currentUserHash, AccessLevel.Owner, "Project creator");

                return (ProjectAccess.OwnerAccess, currentUserHash);
            }

            var currentAccessLevel = GetCurrentAccess(projectId, currentUserHash);
            var shareKeyData = GetShareKeyDataIfStillValid(projectId, shareKey);

            if (ShareKeyIncreasesAccessLevel(currentAccessLevel, shareKeyData?.accessLevel))
            {
                ProvideAccess(projectId, currentUserHash, shareKeyData!.accessLevel, shareKeyData.description);
                if (shareKeyData.isSingleUse)
                {
                    MarkShareKeyAsUsed(shareKeyData.serverId, currentUserHash);
                }
                return (shareKeyData!.accessLevel.ToProjectAccess(), currentUserHash);
            }

            if (!currentAccessLevel.HasValue) return (ProjectAccess.NoAccess, currentUserHash);

            return (currentAccessLevel.Value.ToProjectAccess(), currentUserHash);
        }

        private void ProvideAccess(string projectId, string currentUserHash, AccessLevel accessLevel, string description)
        {

        }

        private AccessLevel? GetCurrentAccess(string projectId, string currentUserHash)
        {
            return null;
        }

        private DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string? shareKey)
        {
            //Check not already used up

            //Check not expired

            //Check cache

            //Materialise file events

            return null;
        }

        private bool ShareKeyIncreasesAccessLevel(AccessLevel? currentAccessLevel, AccessLevel? shareKeyAccessLevel)
        {
            return true;
        }

        private void MarkShareKeyAsUsed(long shareKeyEventServerId, string currentUserHash)
        {

        }
    }
}
