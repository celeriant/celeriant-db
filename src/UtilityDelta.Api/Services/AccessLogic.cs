using Microsoft.AspNetCore.Mvc;
using NanoidDotNet;
using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class AccessLogic(ICrypto crypto, IWriteEvents writeEvents) : IAccessLogic
    {
        public DtoShare CreateShareLink(
            string pi,
            string publicKey,
            string nonce,
            string sign,
            bool isOwner,
            bool singleUse,
            string? description,
            long expiresOn,
            bool readOnly)
        {
            crypto.ValidateWithPublicKey(publicKey, nonce, sign);
            //TODO: Verify access to project

            var createdBy = publicKey.CalculateHash();

            var code = Nanoid.Generate();
            var hashedCode = code.CalculateHash();

            var tp = singleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink;
            var accessLevel = isOwner ? AccessLevel.Owner : readOnly ? AccessLevel.Viewer : AccessLevel.Contributor;

            var shareEvent = new ProjectEventItem(0, createdBy, 0, null, tp,
                description, accessLevel.ToString(), null, expiresOn);

            var (lastServerId, eventDate) = writeEvents.Write([shareEvent], createdBy, pi);

            shareEvent = new ProjectEventItem(lastServerId, createdBy, eventDate, null, tp,
                description, accessLevel.ToString(), null, expiresOn);

            return new DtoShare(code, shareEvent);
        }

        public ProjectAccess IsProjectExistAndHasAccess(
            string projectId,
            bool createProjectIfNotExists,
            string? shareKey,
            string currentUserHash)
        {
            return ProjectAccess.NoAccess;
        }
    }
}
