namespace UtilityDelta.Api.Shared
{
    public class ProjectToUserAccessLevel
    {
        private readonly Dictionary<string, AccessLevel> _users = [];

        public bool IsActiveCache { get; set; }

        public void UpdateCacheForUser(string currentUserHash, AccessLevel? accessLevel, bool allowOverrideExisting)
        {
            var hasExistingEntry = _users.TryGetValue(currentUserHash, out var userEntry);

            //No op - Providing NO access and currently has NO access
            if (!hasExistingEntry && !accessLevel.HasValue)
            {
                return;
            }

            //Currently has NO access but we are providing some form of access
            if (!hasExistingEntry && accessLevel.HasValue)
            {
                _users.Add(currentUserHash, accessLevel.Value);
                return;
            }

            //No op - has access already and allowed to take it away
            if (hasExistingEntry && !accessLevel.HasValue && !allowOverrideExisting)
            {
                return;
            }

            //Currently has access and we want to take it away
            if (hasExistingEntry && !accessLevel.HasValue && allowOverrideExisting)
            {
                _users.Remove(currentUserHash);
                return;
            }

            //Currently has access and we can update it (either higher access level or can override existing entry)
            if (hasExistingEntry && accessLevel.HasValue && (userEntry.IncreasesAccessLevel(accessLevel) || allowOverrideExisting))
            {
                _users[currentUserHash] = accessLevel.Value;
            }
        }

        public AccessLevel? CurrentAccessLevelForUser(string currentUserHash) => 
            _users.TryGetValue(currentUserHash, out var userEntry) ? userEntry : null;
    }
}
