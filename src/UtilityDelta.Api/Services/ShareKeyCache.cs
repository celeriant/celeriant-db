using Microsoft.Extensions.Options;
using NanoidDotNet;
using System;
using System.Collections.Concurrent;
using System.Threading;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ShareKeyCache(IWriteAndBackup writeAndBackup, IReadEvents readEvents, IOptions<ConfigurationEntry> utilityDeltaConfiguration) : IShareKeyCache
    {
        private ConcurrentQueue<string> _cacheQueue = new();
        private ConcurrentDictionary<string, ProjectToShareKeys> _cache = new();
        private DateTime _lastClearedCache = DateTime.UtcNow;
        private TimeSpan _cacheCheckTime = TimeSpan.FromHours(utilityDeltaConfiguration.Value.CACHE_CHECK_TIME_HOURS);

        private void ClearCache()
        {
            if (DateTime.UtcNow.Subtract(_lastClearedCache) < _cacheCheckTime) return;

            if (_cache.Count < utilityDeltaConfiguration.Value.CACHE_MAX_PROJECT_COUNT) return;

            lock (_cacheQueue)
            {
                if (_cache.Count < utilityDeltaConfiguration.Value.CACHE_MAX_PROJECT_COUNT) return;

                _lastClearedCache = DateTime.UtcNow;

                while (_cache.Count > utilityDeltaConfiguration.Value.CACHE_MAX_PROJECT_COUNT)
                {
                    if (!_cacheQueue.TryDequeue(out var projectId)) break;
                    _cache.TryRemove(projectId, out _);
                }
            }
        }

        public DtoShare CreateShareLink(
            string projectId,
            string currentUserHash,
            bool isOwner,
            bool isSingleUse,
            string? iv,
            string? description,
            long expiresOn,
            bool readOnly,
            CancellationToken cancellationToken)
        {
            var code = Nanoid.Generate();
            var hashedCode = code.CalculateHash();
            var tp = isSingleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink;
            var accessLevel = isOwner ? AccessLevel.Owner : readOnly ? AccessLevel.Viewer : AccessLevel.Contributor;
            var shareEvent = new ProjectEventItem(0, currentUserHash, 0, iv, tp, t1: description, t2: accessLevel.ToString(), t3: hashedCode, n1: expiresOn > 0 ? expiresOn : null);

            var projectCache = GetOrBuildCache(projectId, cancellationToken);
            lock (projectCache)
            {
                if (projectCache.Count > utilityDeltaConfiguration.Value.CACHE_MAX_SHARE_LINKS_PER_PROJECT) return new DtoShare(null, null);

                //Write the share event to the log
                shareEvent = writeAndBackup.WriteServerEvent(shareEvent, projectId);

                //Update share cache - note ProjectToShareKeys is not thread-safe
                projectCache.AddShareKey(new DtoShareKeyData(expiresOn == 0 ? null : expiresOn.FromUnixTimeSeconds(), accessLevel, iv, description, hashedCode, isSingleUse, currentUserHash));

                return new DtoShare(code, shareEvent);
            }
        }

        private ProjectToShareKeys GetOrBuildCache(string projectId, CancellationToken cancellationToken)
        {
            ClearCache();

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

                _cacheQueue.Enqueue(projectId);
                PopulateCache(projectId, projectLookup!, cancellationToken);
                return projectLookup;
            }
        }

        private void PopulateCache(string projectId, ProjectToShareKeys projectCache, CancellationToken cancellationToken)
        {
            var relevantEvents = readEvents.Read(projectId, 0, cancellationToken, null, null, [ProjectEventType.AddShareLink, ProjectEventType.AddSingleUseShareLink, ProjectEventType.DisableShareLink]);

            foreach (var eventItem in relevantEvents.events)
            {
                switch (eventItem.tp)
                {
                    case ProjectEventType.AddShareLink:
                    case ProjectEventType.AddSingleUseShareLink:
                        projectCache.AddShareKey(new DtoShareKeyData(
                            expiresOn: eventItem.n1 == null ? null : ((long)eventItem.n1.Value).FromUnixTimeSeconds(),
                            accessLevel: Enum.Parse<AccessLevel>(eventItem.t2!),
                            iv: eventItem.iv,
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

        public DtoShareKeyData? GetShareKeyDataIfStillValid(string projectId, string shareKeyHash, CancellationToken cancellationToken)
        {
            var projectCache = GetOrBuildCache(projectId, cancellationToken);

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

        public ProjectEventItem? MarkShareKeyAsUsed(string projectId, string? currentUserHash, string shareKeyHash, CancellationToken cancellationToken)
        {
            //Update share link cache - mark as used up
            var projectCache = GetOrBuildCache(projectId, cancellationToken);

            lock (projectCache)
            {
                if (projectCache.DisableShareKey(shareKeyHash))
                {
                    //Write used up event to log
                    var eventItem = new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.DisableShareLink, shareKeyHash, null, null, null);
                    eventItem = writeAndBackup.WriteServerEvent(eventItem, projectId);

                    return eventItem;
                }
            }

            return null;
        }
    }
}
