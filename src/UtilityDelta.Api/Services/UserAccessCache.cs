using System.Collections.Concurrent;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class UserAccessCache(IWriteEvents writeEvents, IReadEvents readEvents) : IUserAccessCache
    {
        private ConcurrentDictionary<string, ProjectToUserAccessLevel> _cache = new();

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
            eventItem = writeEvents.WriteServerEvent(eventItem, projectId);

            //Update access cache
            var projectLookup = _cache.GetOrAdd(projectId, (x) => new ProjectToUserAccessLevel());
            lock (projectLookup)
            {
                projectLookup.UpdateCacheForUser(forUserId, potentialAccessLevel, allowDowngrade);
            }

            return eventItem;
        }

        public AccessLevel? GetCurrentAccess(string projectId, string currentUserHash)
        {
            //Check cache
            var isInCache = _cache.TryGetValue(projectId, out var projectLookup);
            if (isInCache && projectLookup != null)
            {
                return projectLookup.CurrentAccessLevelForUser(currentUserHash);
            }

            //If not in cache, materialise and then cache project
            projectLookup = _cache.GetOrAdd(projectId, (x) => new ProjectToUserAccessLevel());
            lock (projectLookup)
            {
                if (projectLookup.IsActiveCache)
                {
                    return projectLookup.CurrentAccessLevelForUser(currentUserHash);
                }

                PopulateUserAccessLevelCache(readEvents, projectId, projectLookup!);
                return projectLookup.CurrentAccessLevelForUser(currentUserHash);
            }
        }

        private static void PopulateUserAccessLevelCache(IReadEvents readEvents, string projectId, ProjectToUserAccessLevel projectLookup)
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
        }
    }
}