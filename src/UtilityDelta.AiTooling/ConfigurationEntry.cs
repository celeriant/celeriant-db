using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling
{
    public class ConfigurationEntry : SystemSettings
    {
        public string OPENAI_API_KEY { get; set; } = string.Empty;
        public string UD_ASSISTANT_ID { get; set; } = string.Empty;
        public string LLM_MODEL { get; set; } = string.Empty;
    }
}
