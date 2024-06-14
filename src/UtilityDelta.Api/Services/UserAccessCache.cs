using System.Collections.Concurrent;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class UserAccessCache(IWriteEvents writeEvents, IReadEvents readEvents) : IUserAccessCache
    {
        private static ConcurrentQueue<string> _cacheQueue = new();
        private static ConcurrentDictionary<string, ProjectToUserAccessLevel> _cache = new();
        private static DateTime _lastClearedCache = DateTime.UtcNow;
        private static TimeSpan _cacheCheckTime = TimeSpan.FromHours(1);
        private const int MAX_CACHE_COUNT = 10000000;
        private const int MAX_USERS = 10000;

        private static void ClearCache()
        {
            if (DateTime.UtcNow.Subtract(_lastClearedCache) < _cacheCheckTime) return;

            if (_cache.Count < MAX_CACHE_COUNT) return;

            lock (_cacheQueue)
            {
                if (_cache.Count < MAX_CACHE_COUNT) return;

                _lastClearedCache = DateTime.UtcNow;

                while (_cache.Count > MAX_CACHE_COUNT)
                {
                    if (!_cacheQueue.TryDequeue(out var projectId)) break;
                    _cache.TryRemove(projectId, out _);
                }
            }
        }

        private ProjectToUserAccessLevel GetOrBuildCache(string projectId)
        {
            ClearCache();

            //Check cache
            var isInCache = _cache.TryGetValue(projectId, out var projectLookup);
            if (isInCache && projectLookup != null)
            {
                return projectLookup;
            }

            //If not in cache, materialise and then cache project
            projectLookup = _cache.GetOrAdd(projectId, (x) => new ProjectToUserAccessLevel());
            lock (projectLookup)
            {
                if (projectLookup.IsActiveCache)
                {
                    return projectLookup;
                }

                _cacheQueue.Enqueue(projectId);
                PopulateCache(projectId, projectLookup!);
                return projectLookup;
            }
        }

        public ProjectEventItem? UpdateAccess(string projectId, string? currentUserHash, string forUserId, AccessLevel? potentialAccessLevel, string? description, bool allowDowngrade, string? shareKey)
        {
            //Not allowed to downgrade your own permissions
            if (allowDowngrade && currentUserHash == forUserId)
            {
                return null;
            }

            var currentAccess = GetCurrentAccess(projectId, forUserId);

            //No op as same permission level or lower level and not downgrading
            if (currentAccess == potentialAccessLevel || !allowDowngrade && !currentAccess.IncreasesAccessLevel(potentialAccessLevel)) 
            {
                return null;
            }

            var eventItem = new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.ProvideAccess, description, forUserId, shareKey, (double?)potentialAccessLevel);

            var projectCache = GetOrBuildCache(projectId);
            lock (projectCache)
            {
                if (projectCache.Count > MAX_USERS) return null;

                //Write the user access event to the log
                eventItem = writeEvents.WriteServerEvent(eventItem, projectId);

                //Update access cache
                projectCache.UpdateCacheForUser(forUserId, potentialAccessLevel, allowDowngrade);

                return eventItem;
            }
        }

        public AccessLevel? GetCurrentAccess(string projectId, string currentUserHash)
        {
            var projectLookup = GetOrBuildCache(projectId);
            lock (projectLookup)
            {
                return projectLookup.CurrentAccessLevelForUser(currentUserHash);
            }
        }

        private void PopulateCache(string projectId, ProjectToUserAccessLevel projectLookup)
        {
            var relevantEvents = readEvents.Read(projectId, 0, null, ProjectEventType.ProvideAccess);

            foreach (var eventItem in relevantEvents.events)
            {
                if (eventItem.t2 == null) continue; //Shouldn't happen but just in case

                switch (eventItem.tp)
                {
                    case ProjectEventType.RemoveProjectMember:
                        projectLookup.UpdateCacheForUser(eventItem.t2!, null, true);
                        break;

                    case ProjectEventType.ProvideAccess:
                        projectLookup.UpdateCacheForUser(eventItem.t2!, (AccessLevel?)eventItem.n1, true);
                        break;
                }
            }

            projectLookup.IsActiveCache = true;
        }
    }
}