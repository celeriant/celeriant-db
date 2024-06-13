using NanoidDotNet;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ShareKeyCache(IWriteEvents writeEvents) : IShareKeyCache
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

            var shareEvent = new ProjectEventItem(0, currentUserHash, 0, null, tp, description, accessLevel.ToString(), hashedCode, expiresOn);
            shareEvent = writeEvents.WriteServerEvent(shareEvent, pi);

            //Update share cache

            return new DtoShare(code, shareEvent);
        }

        public DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string? shareKey)
        {
            //Check not already used up

            //Check not expired

            //Check cache

            //Materialise file events

            return new DtoShareKeyData(0, null, AccessLevel.Viewer, null, "lkdsljkf", false, "kjlkdsfkjl");

            return null;
        }

        public void MarkShareKeyAsUsed(string projectId, long shareKeyEventServerId, string currentUserHash)
        {
            //Write used up event to log
            var eventItem = new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.DisableShareLink, null, null, null, shareKeyEventServerId);
            eventItem = writeEvents.WriteServerEvent(eventItem, projectId);

            //TODO: Update share link cache - mark as used up
        }
    }
}
