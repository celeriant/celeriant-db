namespace UtilityDelta.Api.Shared
{
    public static class Constants
    {
        public const string SUB_DIR_CONTAINERS = "/mnt/nvme0n1p1/containers";

        public const int OFFSET_BYTES_FOR_GETTING_EVENTID = 
            sizeof(long) //Event id
            + SIZEOF_EVENT_SIZE; //Total size

        public const int SIZEOF_EVENT_SIZE = sizeof(int);
    }
}
