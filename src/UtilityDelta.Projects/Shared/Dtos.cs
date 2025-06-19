namespace UtilityDelta.Projects.Shared
{
    public record DtoRead(List<ProjectEventItem> events, long serverId);

    public record DtoWrite(long serverId, long eventDate);

    public record DtoShare(string? shareKey, ProjectEventItem? shareEvent);

    public record DtoDisableAccess(ProjectEventItem? disableAccessEvent);

    public record DtoDeleteProject(bool success);

    public record DtoAccessInfo(ProjectAccess ProjectAccess, string CurrentUserHash);

    public record DtoShareKeyData(DateTime? expiresOn, AccessLevel accessLevel, string? iv, string? description, string hashedCode, bool isSingleUse, string createdBy);

    public record ProjectEventItem(long si, string? cb, long ed, string? iv, ProjectEventType tp, string? t1, string? t2, string? t3, double? n1);
}
