using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
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
            var prompt = InitialPrompt(dtoBreakdownInputs);

            //prompt.Append(" After listing all the tasks, also list any dependencies between the tasks, one line at a time, in a similar format, using '->'.");

            var requestData = new LlmInput
            {
                model = "llama3",
                prompt = prompt,
                stream = false
            };

            var conversationHistory = new List<string>();
            conversationHistory.Add($"User: {requestData.prompt}");

            string jsonData = JsonSerializer.Serialize(requestData, ReadSerializerContext.Default.LlmInput);

            string response = await StreamResponse($"http://{utilityDeltaConfiguration.Value.LLM_SERVER}/api/generate", jsonData);
            conversationHistory.Add($"LLM: {response}");
            var llmResult1 = JsonSerializer.Deserialize<LlmResult>(response, ReadSerializerContext.Default.LlmResult);
            var r1 = llmResult1.response;

            conversationHistory.Add($"User: List any dependencies between the tasks, one line at a time, in a similar format, using '->'");

            requestData = new LlmInput
            {
                model = "llama3",
                prompt = string.Join("\n", conversationHistory),
                stream = false
            };
            jsonData = JsonSerializer.Serialize(requestData, ReadSerializerContext.Default.LlmInput);
            response = await StreamResponse($"http://{utilityDeltaConfiguration.Value.LLM_SERVER}/api/generate", jsonData);
            var llmResult2 = JsonSerializer.Deserialize<LlmResult>(response, ReadSerializerContext.Default.LlmResult);
            var r2 = llmResult2.response;
            DtoBreakdownOutputs result = BuildResult(dtoBreakdownInputs, r1, r2);

            return result;
        }

        public static DtoUnknownOutputs BuildUnknownResult(DtoBreakdownInputs dtoBreakdownInputs, string r1)
        {
            var result = new DtoUnknownOutputs();
            var subtasks = new List<string>();

            var numbers = new List<int>();
            foreach (var output in r1.Split('\n'))
            {
                if (string.IsNullOrWhiteSpace(output) || !int.TryParse(output, out var taskNumber)) continue;

                numbers.Add(taskNumber);
            }
            result.unkownTasks = numbers.ToArray();
            return result;
        }

        public static DtoRolesOutputs BuildRolesResult(string r1)
        {
            var output = new List<string>();
            foreach (var text in r1.Split('\n'))
            {
                var clean = text.Trim();
                if (text.StartsWith('-'))
                {
                    clean = clean.Substring(1);
                }
                output.Add(clean.Trim());
            }

            return new DtoRolesOutputs() { roles = output.ToArray() };
        }

        public static DtoBreakdownOutputs BuildResult(DtoBreakdownInputs dtoBreakdownInputs, string r1, string r2)
        {
            var result = new DtoBreakdownOutputs();
            var subtasks = new List<string>();

            var dependencyMode = false;
            foreach (var output in r1.Split('\n'))
            {
                if (output.ToLowerInvariant() == dtoBreakdownInputs.task.ToLowerInvariant() || string.IsNullOrWhiteSpace(output)) continue;

                subtasks.Add(output);
            }
            var predecessors = new List<string>();
            var successors = new List<string>();

            foreach (var output in r2.Split('\n'))
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

        public static string DiscoverUnknownsPrompt(DtoBreakdownInputs dtoBreakdownInputs)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Identify tasks in this list that could take longer than {dtoBreakdownInputs.minDuration} hours to complete. Only return the task number, one line for each task number. Do not return any other text.");

            prompt.Append($" For context, the parents of these tasks are: \"{dtoBreakdownInputs.task}\"");
            if (dtoBreakdownInputs.parents != null && dtoBreakdownInputs.parents.Length > 0)
            {
                foreach (var parent in dtoBreakdownInputs.parents)
                {
                    prompt.Append(" and then ");
                    prompt.Append($"\"{parent}\"");
                }
            }
            prompt.AppendLine(".");

            prompt.AppendLine(" Here are the tasks: ");
            for (var i = 0; i < dtoBreakdownInputs.siblings.Length; i++)
            {
                prompt.AppendLine($"{i} - {dtoBreakdownInputs.siblings[i]}");
            }

            return prompt.ToString();
        }

        public static string DetermineRolesPrompt(DtoRolesInputs dtoRolesInputs)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Here is a rough breakdown of my project - tasks and sub-tasks. I need to hire staff to complete this project. Tell me what roles I need to hire for. Only stick to popular roles that I can hire for on the market. Only return the role title, one line for each role. Do not return any numbering, formatting, special characters or any other text.");

            foreach (var task in dtoRolesInputs.tasks)
            {
                prompt.AppendLine(task);
            }

            return prompt.ToString();
        }

        public static string InitialPrompt(DtoBreakdownInputs dtoBreakdownInputs)
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

            return prompt.ToString();
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

        public Task<DtoUnknownOutputs> IdentifyUnknowns(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<DtoRolesOutputs> DetermineRoles(DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public static string AssignRolesPrompt(DtoAssignRolesInputs dtoRolesInputs)
        {
            var prompt = new StringBuilder();

            prompt.AppendLine("Our project has the following defined roles that can action tasks:");
            foreach (var roleText in dtoRolesInputs.roles)
            {
                prompt.AppendLine(roleText);
            }
            prompt.AppendLine();

            prompt.AppendLine(" Here are the tasks: ");

            for (var i = 0; i < dtoRolesInputs.tasks.Length; i++)
            {
                prompt.AppendLine($"{i} - {dtoRolesInputs.tasks[i]}");
            }
            prompt.AppendLine();

            prompt.AppendLine($"Determine the primary role to assign to each task. If not sure, or the task requires multiple roles, skip that task. Only return the task number and then role, using '->' to separate, one line for each. Do not return any other text.");

            return prompt.ToString();
        }

        public static DtoAssignRolesOutputs AssignRolesResult(DtoAssignRolesInputs dtoRolesInputs, string r1)
        {
            var taskIds = new List<int>();
            var roles = new List<string>();

            var entries = r1.Split('\n');
            foreach (var output in entries)
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

                var taskIdStr = depSplit[0].Trim();
                var roleText = depSplit[1].Trim();

                if (!int.TryParse(taskIdStr, out var taskId)) continue;

                taskIds.Add(taskId);
                roles.Add(roleText);
            }

            return new DtoAssignRolesOutputs() { roles = roles.ToArray(), taskNumbers = taskIds.ToArray() };
        }

        public Task<DtoAssignRolesOutputs> AssignRoles(DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
