namespace UtilityDelta.WebAPI.Entities
{
    public class AccessLinkItem
    {
        public string pi { get; set; } = string.Empty;
        public string id { get; set; } = string.Empty;
        public long created { get; set; } = 0;
        public long validUntil { get; set; } = 0;
        public bool isOwner { get; set; }
        public bool singleUse { get; set; }
        public bool readOnly { get; set; }
        public string? description { get; set; }
        public string? cb { get; set; }
    }
}
