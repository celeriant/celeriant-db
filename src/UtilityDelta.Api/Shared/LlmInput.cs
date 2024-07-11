namespace UtilityDelta.Api.Shared
{
    public class LlmInput
    {
        public string model { get; set; }
        public string prompt { get; set; }
        public bool stream { get; set; }
    }
}
