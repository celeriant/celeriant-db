namespace UtilityDelta.Api.Shared
{
    public class LlmResult
    {
        public string model { get; set; }
        public string created_at { get; set; }
        public string response { get; set; }
        public bool done { get; set; }
        public string done_reason { get; set; }
        public int[] context { get; set; }
        public long total_duration { get; set; }
        public long load_duration { get; set; }
        public int prompt_eval_count { get; set; }
        public int prompt_eval_duration { get; set; }
        public int eval_count { get; set; }
        public int eval_duration { get; set; }
    }
}
