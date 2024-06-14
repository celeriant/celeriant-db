namespace UtilityDelta.Api.Shared
{
    public class ConfigurationEntry
    {
        public string SUB_DIR_CONTAINERS { get; set; } = string.Empty;
        public int FILE_HANDLE_OPEN_LIMIT { get; set; }
        public int CACHE_MAX_USERS_PER_PROJECT { get; set; }
        public int CACHE_MAX_SHARE_LINKS_PER_PROJECT { get; set; }
        public int CACHE_MAX_PROJECT_COUNT { get; set; }
        public double CACHE_CHECK_TIME_HOURS { get; set; }
        public string[] CORS_SITES { get; set; } = [];
    }
}
