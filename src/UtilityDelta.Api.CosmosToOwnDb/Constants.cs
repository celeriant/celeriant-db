namespace UtilityDelta.WebAPI
{
    public static class Constants
    {
        public const string OAUTH_NAME_IDENTIFYER = "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier";
        public const string OAUTH_NAME = "name";

        public const string COSMOS_PARTITION_PROJECTS = "projects";

        public static double MAX_NONCE_TIME_MINUMTES = 2.0;

        public const string COSMOS_DATABASEID = "utilitydelta";
        public const string COSMOS_CONTAINERID_EVENTS = "events";
        public const string COSMOS_CONTAINERID_PROJECTACCESS = "projectaccess";
        public const string COSMOS_CONTAINERID_SHAREKEYS = "sharekeys";

        public const string SIGNALR_ENDPOINT = "/realtime";
        
        public const int HOURS_BEFORE_ACCESSLINK_EXPIRES = 24;

    }
}
