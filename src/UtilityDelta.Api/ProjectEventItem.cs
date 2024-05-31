namespace UtilityDelta.Api
{
    public class ProjectEventItem
    {
        public ulong id { get; set; }
        public byte[]? cb { get; set; }
        public long ed { get; set; }
        public byte[]? iv { get; set; }
        public ProjectEventType tp { get; set; } = ProjectEventType.AddTask;
        public string? t1 { get; set; } = null;
        public string? t2 { get; set; } = null;
        public string? t3 { get; set; } = null;
        public double? n1 { get; set; } = null;
    }
}
