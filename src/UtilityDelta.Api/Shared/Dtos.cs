namespace UtilityDelta.Api.Shared
{
    public record DtoRead(List<ProjectEventItem> events, long serverId);

    public record DtoWrite(long serverId, long eventDate);

    public record DtoShare(string? shareKey, ProjectEventItem? shareEvent);

    public record DtoDisableAccess(ProjectEventItem? disableAccessEvent);

    public record DtoAccessInfo(ProjectAccess ProjectAccess, string CurrentUserHash);

    public record DtoShareKeyData(DateTime? expiresOn, AccessLevel accessLevel, string? iv, string? description, string hashedCode, bool isSingleUse, string createdBy);

    public record ProjectEventItem(long serverId, string? cb, long ed, string? iv, ProjectEventType tp, string? t1, string? t2, string? t3, double? n1);

    public class DtoBreakdownInputs
    {
        public string? system {  get; set; }
        public string task { get; set; }
        public string[] parents { get; set; }
        public string[] siblings { get; set; }
        public double minDuration { get; set; }
    }

    public class DtoAssignRolesInputs
    {
        public string? system { get; set; }
        public string[] parents { get; set; }
        public string[] tasks { get; set; }
        public string[] roles { get; set; }
    }

    public class DtoOrganiseInputs
    {
        public string? system { get; set; }
        public string[] parents { get; set; }
        public string[] tasks { get; set; }
    }

    public class DtoRolesInputs
    {
        public string? system { get; set; }
        public string[] tasks { get; set; }
    }

    public class DtoBreakdownOutputs
    {
        public string[] subTasks { get; set; }
        public string[] predecessors { get; set; }
        public string[] successors { get; set; }
    }

    public class DtoUnknownOutputs
    {
        public int[] unkownTasks { get; set; }
    }

    public class DtoAssignRolesOutputs
    {
        public int[] taskNumbers { get; set; }
        public string[] roles { get; set; }
    }

    public class DtoOrganiseOutputs
    {
        public int[] taskNumbers { get; set; }
        public string[] taskGroups { get; set; }
    }

    public class DtoRolesOutputs
    {
        public string[] roles { get; set; }
    }
}
