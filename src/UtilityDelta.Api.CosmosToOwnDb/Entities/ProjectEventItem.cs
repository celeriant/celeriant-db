using UtilityDelta.WebAPI.Data;

namespace UtilityDelta.WebAPI.Entities
{
    public class ProjectEventItem
    {
        public string id { get; set; } = string.Empty;
        public string pi { get; set; } = string.Empty;
        public string? cb { get; set; }
        public long ed { get; set; } = 0;
        public string? iv { get; set; } = null;
        public ProjectEventType tp { get; set; } = ProjectEventType.AddTask;
        public string? t1 { get; set; } = null;
        public string? t2 { get; set; } = null;
        public string? t3 { get; set; } = null;
        public double? n1 { get; set; } = null;
    }
}
