using Microsoft.Extensions.Options;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class LlmBreakdown(IOptions<ConfigurationEntry> utilityDeltaConfiguration) : ILlmBreakdown
    {
        public async Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, string currentUserHash, string pi, CancellationToken cancellationToken)
        {
            var prompt = new StringBuilder();
            prompt.Append($"Breakdown the task \"{dtoBreakdownInputs.task}\" into sub-tasks for my project. Only output one level of breakdown, from 2 to a maximum 10 sub-tasks. Don't include sub-tasks that take less than {dtoBreakdownInputs.minDuration} hours to complete. Do not include the input task or any other content other than the title of each task. Do not include numbering or any special characters or any intro sentence.");

            if (dtoBreakdownInputs.parents != null && dtoBreakdownInputs.parents.Length > 0)
            {
                prompt.Append(" For context, the parents of this task is ");
                var isFirst = true;
                foreach (var parent in dtoBreakdownInputs.parents) 
                { 
                    if (!isFirst)
                    {
                        prompt.Append(" and then ");
                    }
                    prompt.Append($"\"{parent}\""); 
                    isFirst = false;
                }
                prompt.Append(".");
            }

            if (dtoBreakdownInputs.siblings != null && dtoBreakdownInputs.siblings.Length > 0)
            {
                prompt.Append(" Don't include these tasks as we already have them in the project: ");
                var isFirst = true;
                foreach (var sibling in dtoBreakdownInputs.siblings)
                {
                    if (!isFirst)
                    {
                        prompt.Append(" and ");
                    }
                    prompt.Append($"\"{sibling}\"");
                    isFirst = false;
                }
                prompt.Append(".");
            }

            //prompt.Append(" After listing all the tasks, also list any dependencies between the tasks, one line at a time, in a similar format, using '->'.");

            var requestData = new LlmInput
            {
                model = "llama3",
                prompt = prompt.ToString(),
                stream = false
            };

            var conversationHistory = new List<string>();
            conversationHistory.Add($"User: {requestData.prompt}");

            string jsonData = JsonSerializer.Serialize(requestData, ReadSerializerContext.Default.LlmInput);

            string response = await StreamResponse($"http://{utilityDeltaConfiguration.Value.LLM_SERVER}/api/generate", jsonData);
            conversationHistory.Add($"LLM: {response}");
            var llmResult = JsonSerializer.Deserialize<LlmResult>(response, ReadSerializerContext.Default.LlmResult);

            var result = new DtoBreakdownOutputs();
            var subtasks = new List<string>();

            var dependencyMode = false;
            foreach (var output in llmResult!.response.Split('\n'))
            {
                if (output.ToLowerInvariant() == dtoBreakdownInputs.task.ToLowerInvariant()) continue;

                if (string.IsNullOrWhiteSpace(output) || output.ToLowerInvariant().StartsWith("dependencies"))
                {
                    dependencyMode = true;
                    continue;
                }
                if (!dependencyMode)
                {
                    subtasks.Add(output);
                } else
                {
                    //var depOutput = output;
                    //if (output.StartsWith("->"))
                    //{
                    //    depOutput = output.Substring(2).Trim();
                    //}
                    //if (output.StartsWith(">"))
                    //{
                    //    depOutput = output.Substring(1).Trim();
                    //}
                    //var depSplit = depOutput.Split("->");
                    //if (depSplit.Length != 2) continue;

                    //predecessors.Add(depSplit[0].Trim());
                    //successors.Add(depSplit[1].Trim());
                }
            }

            conversationHistory.Add($"User: List any dependencies between the tasks, one line at a time, in a similar format, using '->'");

            requestData = new LlmInput
            {
                model = "llama3",
                prompt = string.Join("\n", conversationHistory),
                stream = false
            };
            jsonData = JsonSerializer.Serialize(requestData, ReadSerializerContext.Default.LlmInput);
            response = await StreamResponse($"http://{utilityDeltaConfiguration.Value.LLM_SERVER}/api/generate", jsonData);
            llmResult = JsonSerializer.Deserialize<LlmResult>(response, ReadSerializerContext.Default.LlmResult);

            var predecessors = new List<string>();
            var successors = new List<string>();

            foreach (var output in llmResult!.response.Split('\n'))
            {
                var depOutput = output;
                if (output.StartsWith("->"))
                {
                    depOutput = output.Substring(2).Trim();
                }
                if (output.StartsWith(">"))
                {
                    depOutput = output.Substring(1).Trim();
                }
                var depSplit = depOutput.Split("->");
                if (depSplit.Length != 2) continue;

                predecessors.Add(depSplit[0].Trim());
                successors.Add(depSplit[1].Trim());
            }

            result.subTasks = subtasks.ToArray();
            result.predecessors = predecessors.ToArray();
            result.successors = successors.ToArray();

            return result;
        }

        public static async Task<string> StreamResponse(string url, string jsonData)
        {
            var client = new HttpClient();
            var content = new StringContent(jsonData, System.Text.Encoding.UTF8, "application/json");

            using (var response = await client.PostAsync(url, content))
            {
                response.EnsureSuccessStatusCode();
                using (var stream = await response.Content.ReadAsStreamAsync())
                using (var reader = new System.IO.StreamReader(stream))
                {
                    string? line;
                    string result = "";
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        result += line + "\n";
                    }
                    return result.Trim();
                }
            }
        }
    }
}
