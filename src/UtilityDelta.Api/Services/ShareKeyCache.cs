using NanoidDotNet;
using System;
using System.Collections.Concurrent;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ShareKeyCache(IWriteEvents writeEvents, IReadEvents readEvents) : IShareKeyCache
    {
        private static ConcurrentDictionary<string, ProjectToShareKeys> _cache = new();

        public DtoShare CreateShareLink(
            string projectId,
            string currentUserHash,
            bool isOwner,
            bool isSingleUse,
            string? description,
            long expiresOn,
            bool readOnly)
        {
            var code = Nanoid.Generate();
            var hashedCode = code.CalculateHash();

            var tp = isSingleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink;
            var accessLevel = isOwner ? AccessLevel.Owner : readOnly ? AccessLevel.Viewer : AccessLevel.Contributor;

            var shareEvent = new ProjectEventItem(0, currentUserHash, 0, null, tp, t1: description, t2: accessLevel.ToString(), t3: hashedCode, n1: expiresOn);
            shareEvent = writeEvents.WriteServerEvent(shareEvent, projectId);

            //Update share cache - note ProjectToShareKeys is not thread-safe
            var projectLookup = _cache.GetOrAdd(projectId, (x) => new ProjectToShareKeys());
            lock (projectLookup)
            {
                projectLookup.AddShareKey(new DtoShareKeyData(expiresOn == 0 ? null : expiresOn.FromUnixTimeSeconds(), accessLevel, description, hashedCode, isSingleUse, currentUserHash));
            }

            return new DtoShare(code, shareEvent);
        }

        private ProjectToShareKeys GetOrBuildCache(string projectId)
        {
            //Check cache
            var isInCache = _cache.TryGetValue(projectId, out var projectLookup);
            if (isInCache && projectLookup != null)
            {
                return projectLookup;
            }

            //If not in cache, materialise and then cache project
            projectLookup = _cache.GetOrAdd(projectId, (x) => new ProjectToShareKeys());
            lock (projectLookup)
            {
                if (projectLookup.IsActiveCache)
                {
                    return projectLookup;
                }

                PopulateCache(projectId, projectLookup!);
                return projectLookup;
            }
        }

        private void PopulateCache(string projectId, ProjectToShareKeys projectCache)
        {
            var relevantEvents = readEvents.Read(projectId, 0, null, null, [ProjectEventType.AddShareLink, ProjectEventType.AddSingleUseShareLink, ProjectEventType.DisableShareLink]);

            foreach (var eventItem in relevantEvents.events)
            {
                switch (eventItem.tp)
                {
                    case ProjectEventType.AddShareLink:
                    case ProjectEventType.AddSingleUseShareLink:
                        projectCache.AddShareKey(new DtoShareKeyData(
                            expiresOn: eventItem.n1 == null ? null : ((long)eventItem.n1.Value).FromUnixTimeSeconds(),
                            accessLevel: Enum.Parse<AccessLevel>(eventItem.t2!),
                            description: eventItem.t1,
                            hashedCode: eventItem.t3!,
                            isSingleUse: eventItem.tp == ProjectEventType.AddSingleUseShareLink,
                            createdBy: eventItem.cb!));
                        break;

                    case ProjectEventType.DisableShareLink:
                        projectCache.DisableShareKey(eventItem.t1!);
                        break;
                }
            }

            projectCache.IsActiveCache = true;
        }

        public DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string shareKeyHash)
        {
            var projectCache = GetOrBuildCache(projectId);

            lock (projectCache)
            {
                var potentialShareKey = projectCache.Find(shareKeyHash);

                //No match, probably already disabled
                if (potentialShareKey == null) return null;

                //Share key already expired
                if (potentialShareKey.expiresOn != null && potentialShareKey.expiresOn.Value < DateTime.UtcNow) return null;

                return potentialShareKey;
            }
        }

        public bool MarkShareKeyAsUsed(string projectId, string shareKeyHash)
        {
            //Update share link cache - mark as used up
            var projectCache = GetOrBuildCache(projectId);
            lock (projectCache)
            {
                if (projectCache.DisableShareKey(shareKeyHash))
                {
                    //Write used up event to log
                    var eventItem = new ProjectEventItem(0, null, 0, null, ProjectEventType.DisableShareLink, shareKeyHash, null, null, null);
                    eventItem = writeEvents.WriteServerEvent(eventItem, projectId);

                    return true;
                }
            }

            return false;
        }
    }
}
