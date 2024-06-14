namespace UtilityDelta.Api.Shared
{
    public record DtoRead(List<ProjectEventItem> events, long serverId);

    public record DtoWrite(long serverId, long eventDate);

    public record DtoShare(string? shareKey, ProjectEventItem? shareEvent);

    public record DtoDisableAccess(ProjectEventItem? disableAccessEvent);

    public record DtoAccessInfo(ProjectAccess ProjectAccess, string CurrentUserHash, ProjectEventItem? AccessEvent);

    public record DtoShareKeyData(DateTime? expiresOn, AccessLevel accessLevel, string? description, string hashedCode, bool isSingleUse, string createdBy);

    public record ProjectEventItem(long serverId, string? cb, long ed, string? iv, ProjectEventType tp, string? t1, string? t2, string? t3, double? n1);

    public class ConfigurationEntry
    {
        public string SUB_DIR_CONTAINERS { get; set; }
        public int FILE_HANDLE_OPEN_LIMIT { get; set; }
        public int CACHE_MAX_USERS_PER_PROJECT { get; set; }
        public int CACHE_MAX_SHARE_LINKS_PER_PROJECT { get; set; }
        public int CACHE_MAX_PROJECT_COUNT { get; set; }
        public double CACHE_CHECK_TIME_HOURS { get; set; }
    }
}
