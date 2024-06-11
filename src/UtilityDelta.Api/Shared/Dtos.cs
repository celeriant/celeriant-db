namespace UtilityDelta.Api.Shared
{
    public record DtoSyncEvents(long serverTime);

    public record DtoShareProject(string shareKey);

    public record ProjectEventItem(long serverId, string? cb, long ed, string? iv, ProjectEventType tp, string? t1, string? t2, string? t3, double? n1);

}
