namespace UtilityDelta.Api.Shared
{
    public record DtoRead(List<ProjectEventItem> events, long serverId);

    public record DtoWrite(long serverId, long eventDate);

    public record DtoShare(string? shareKey, ProjectEventItem? shareEvent);

    public record DtoShareKeyData(long serverId, DateTime expiresOn, AccessLevel accessLevel, string description, string hashedCode, bool isSingleUse);

    public record ProjectEventItem(long serverId, string? cb, long ed, string? iv, ProjectEventType tp, 
        string? t1, string? t2, string? t3, double? n1);

}
