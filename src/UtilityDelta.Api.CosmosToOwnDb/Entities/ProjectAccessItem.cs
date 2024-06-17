namespace UtilityDelta.WebAPI.Entities
{
    public class ProjectAccessItem
    {
        public string id { get; set; } = string.Empty;
        public string pi { get; set; } = string.Empty;
        public bool isOwner { get; set; }
        public bool readOnly { get; set; }
        public string description { get; set; }
        public string shareKey { get; set; }
    }
}
