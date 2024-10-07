using System.Text;
using UtilityDelta.AiTooling.Dtos;

namespace UtilityDelta.AiTooling
{
    public static class PromptEngineering
    {
        public static DtoUnknownOutputs BuildUnknownResult(this DtoBreakdownInputs dtoBreakdownInputs, string r1)
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

        public static DtoBreakdownOutputs ImageBreakdownResult(this DtoImageBreakdownInputs dtoImageBreakdownInputs, string r1)
        {
            var result = new DtoBreakdownOutputs();
            var subtasks = new List<string>();
            var predecessors = new List<string>();
            var successors = new List<string>();

            foreach (var output in r1.Split('\n'))
            {
                var taskOutput = output;
                if (output.StartsWith("->"))
                {
                    taskOutput = output.Substring(2).Trim();
                }
                if (output.StartsWith(">"))
                {
                    taskOutput = output.Substring(1).Trim();
                }
                if (taskOutput.ToLowerInvariant() == dtoImageBreakdownInputs.task!.ToLowerInvariant() || string.IsNullOrWhiteSpace(taskOutput)) continue;

                var depSplit = taskOutput.Split("->");
                if (depSplit.Length != 2)
                {
                    subtasks.Add(taskOutput);
                } else
                {
                    if (depSplit[0].StartsWith('-'))
                    {
                        depSplit[0] = depSplit[0].Substring(1);
                    }
                    predecessors.Add(depSplit[0].Trim());
                    successors.Add(depSplit[1].Trim());
                }
            }

            result.subTasks = subtasks.ToArray();
            result.predecessors = predecessors.ToArray();
            result.successors = successors.ToArray();
            return result;
        }

        public static DtoBreakdownQuestionsOutputs AutoBreakdownInitialQuestionsResult(this DtoBreakdownInputs dtoBreakdownInputs, string r1)
        {
            var result = new DtoBreakdownQuestionsOutputs();
            var subtasks = new List<string>();

            foreach (var output in r1.Split('\n'))
            {
                var taskOutput = output.Trim();
                if (taskOutput.StartsWith('-'))
                {
                    taskOutput = taskOutput.Substring(1).Trim();
                }
                if (taskOutput.ToLowerInvariant() == dtoBreakdownInputs.task!.ToLowerInvariant() || string.IsNullOrWhiteSpace(taskOutput)) continue;

                subtasks.Add(taskOutput);
            }

            result.questions = subtasks.ToArray();
            return result;
        }

        public static DtoBreakdownOutputs AutoBreakdownResult(this DtoBreakdownInputs dtoBreakdownInputs, string r1, string r2)
        {
            var result = new DtoBreakdownOutputs();
            var subtasks = new List<string>();

            foreach (var output in r1.Split('\n'))
            {
                var taskOutput = output.Trim();
                if (taskOutput.StartsWith('-'))
                {
                    taskOutput = taskOutput.Substring(1).Trim();
                }
                if (taskOutput.ToLowerInvariant() == dtoBreakdownInputs.task!.ToLowerInvariant() || string.IsNullOrWhiteSpace(taskOutput)) continue;

                subtasks.Add(taskOutput);
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

        public static string DiscoverUnknownsPrompt(this DtoBreakdownInputs dtoBreakdownInputs)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Identify tasks in this list that could take longer than 15 story points to complete. Only return the task number, one line for each task number. Do not return any other text.");

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

        public static string DetermineRolesPrompt(this DtoRolesInputs dtoRolesInputs)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Here is a rough breakdown of my project - tasks and sub-tasks. I need to hire staff to complete this project. Tell me what roles I need to hire for. Only stick to popular roles that I can hire for on the market. Only return the role title, one line for each role. Do not return any numbering, formatting, special characters or any other text.");

            foreach (var task in dtoRolesInputs.tasks)
            {
                prompt.AppendLine(task);
            }

            return prompt.ToString();
        }

        public static string AutoBreakdownInitialQuestionsPrompt(this  DtoBreakdownInputs dtoBreakdownInputs, bool utiliseFiles)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine("To better prepare for task breakdown, ask the user some clarifying questions. Each question should be short and able to be answered with a yes or a no by the user. Do not give \"[this] or [that]\" style questions as the user can only answer yes or no. Do not number the questions or add any bullet points.");
            prompt.AppendLine($"The task is \"{dtoBreakdownInputs.task}\".");
            if (utiliseFiles)
            {
                prompt.AppendLine($"For context, utilise the provided files when breaking down the task.");
            }
            prompt.AppendLine();

            if (dtoBreakdownInputs.parents != null && dtoBreakdownInputs.parents.Length > 0)
            {
                prompt.Append("For context, the parents of this task is ");
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
                prompt.AppendLine(".");
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.siblings != null && dtoBreakdownInputs.siblings.Length > 0)
            {
                prompt.AppendLine("The task has already been broken down previously, here are the sub-tasks: ");
                foreach (var sibling in dtoBreakdownInputs.siblings)
                {
                    prompt.AppendLine($" - {sibling}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.yesQuestions != null && dtoBreakdownInputs.yesQuestions.Length > 0)
            {
                prompt.AppendLine("The user has already indicated 'YES' for the following questions: ");
                foreach (var question in dtoBreakdownInputs.yesQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.noQuestions != null && dtoBreakdownInputs.noQuestions.Length > 0)
            {
                prompt.AppendLine("The user has already indicated 'NO' for the following questions: ");
                foreach (var question in dtoBreakdownInputs.noQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.unsureQuestions != null && dtoBreakdownInputs.unsureQuestions.Length > 0)
            {
                prompt.AppendLine("The user is 'UNSURE' about these questions. Possibly rephrase them, add more detail or just leave them out: ");
                foreach (var question in dtoBreakdownInputs.unsureQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.unansweredQuestions != null && dtoBreakdownInputs.unansweredQuestions.Length > 0)
            {
                prompt.AppendLine("The user has skipped these questions. Just leave them out from future responses: ");
                foreach (var question in dtoBreakdownInputs.unansweredQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (!string.IsNullOrWhiteSpace(dtoBreakdownInputs.userNotes))
            {
                prompt.AppendLine("The user has provided some extra details for context: ");
                prompt.AppendLine(dtoBreakdownInputs.userNotes);
                prompt.AppendLine();
            }

            return prompt.ToString();
        }

        public static string ImageBreakdownPrompt(this DtoImageBreakdownInputs dtoImageBreakdownInputs)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Utilise the provided image to generate sub-tasks for the task \"{dtoImageBreakdownInputs.task}\".");
            prompt.AppendLine($"If the image is a whiteboard diagram, try to infer the hierarchy of tasks out output tasks and sub-tasks (sub-tasks are indented by one space). Otherwise, interpret the context of the image, derive some tasks and determine any dependencies (as described below).");
            prompt.AppendLine($"Don't just write out the text from the image, instead turn the text from each task into a coherent sentence.");
            prompt.AppendLine($"If you see a dashed line with an arrow between two tasks, this is a dependency. List both tasks together in the direction of the arrow, one line at a time, at the end of the prompt response, with '->'. For example:");
            prompt.AppendLine($"My First Task -> My Second Task");
            prompt.AppendLine($"Only output the tasks, one per line, no other text. Each task must start with '-' (markdown format)");
            prompt.AppendLine();

            if (dtoImageBreakdownInputs.parents != null && dtoImageBreakdownInputs.parents.Length > 0)
            {
                prompt.Append("For context, the parents of this task is ");
                var isFirst = true;
                foreach (var parent in dtoImageBreakdownInputs.parents)
                {
                    if (!isFirst)
                    {
                        prompt.Append(" and then ");
                    }
                    prompt.Append($"\"{parent}\"");
                    isFirst = false;
                }
                prompt.AppendLine(".");
                prompt.AppendLine();
            }
            return prompt.ToString();
        }

        public static string AutoBreakdownInitialPrompt(this DtoBreakdownInputs dtoBreakdownInputs, bool utiliseFiles)
        {
            var prompt = new StringBuilder();
            prompt.AppendLine($"Breakdown the task \"{dtoBreakdownInputs.task}\" into sub-tasks for my project.");
            if (utiliseFiles)
            {
                prompt.AppendLine($"For context, utilise the provided files when breaking down the task.");
            }
            prompt.AppendLine();

            if (dtoBreakdownInputs.parents != null && dtoBreakdownInputs.parents.Length > 0)
            {
                prompt.Append("The parents of this task is ");
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
                prompt.AppendLine(".");
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.siblings != null && dtoBreakdownInputs.siblings.Length > 0)
            {
                prompt.AppendLine("Don't include the following tasks as we already have them in the project: ");
                foreach (var sibling in dtoBreakdownInputs.siblings)
                {
                    prompt.AppendLine($" - {sibling}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.yesQuestions != null && dtoBreakdownInputs.yesQuestions.Length > 0)
            {
                prompt.AppendLine("The user has answered 'YES' to the following clarification questions: ");
                foreach (var question in dtoBreakdownInputs.yesQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (dtoBreakdownInputs.noQuestions != null && dtoBreakdownInputs.noQuestions.Length > 0)
            {
                prompt.AppendLine("The user has answered 'NO' to the following clarification questions: ");
                foreach (var question in dtoBreakdownInputs.noQuestions)
                {
                    prompt.AppendLine($" - {question}");
                }
                prompt.AppendLine();
            }

            if (!string.IsNullOrWhiteSpace(dtoBreakdownInputs.userNotes))
            {
                prompt.AppendLine("The user has provided some extra details for context: ");
                prompt.AppendLine(dtoBreakdownInputs.userNotes);
                prompt.AppendLine();
            }

            prompt.AppendLine($"Give an actionable, single sentence per task, without any full stops at the end. Only output one level of breakdown. Don't include sub-tasks that take less than {dtoBreakdownInputs.minDuration} hours to complete. Do not include the input task or any other content other than the title of each task. Do not include numbering or any special characters or any intro sentence. Tasks should be specific and actionable. Do not add fluffy irrelevant tasks.");
            prompt.AppendLine();

            return prompt.ToString();
        }


        public static string AssignRolesPrompt(this DtoAssignRolesInputs dtoRolesInputs)
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

        public static string GroupTasksPrompt(this DtoOrganiseInputs dtoRolesInputs)
        {
            var prompt = new StringBuilder();

            prompt.AppendLine("Group related tasks. Here are the tasks:");
            for (var i = 0; i < dtoRolesInputs.tasks.Length; i++)
            {
                prompt.AppendLine($"{i} - {dtoRolesInputs.tasks[i]}");
            }
            prompt.AppendLine();

            prompt.AppendLine($"Return the group name and the task numbers for that group, one line for each. Do not return any other text. Skip tasks that don't belong to any group.");

            return prompt.ToString();
        }

        public static DtoOrganiseOutputs GroupTasksResult(this DtoOrganiseInputs dtoOrganiseInputs, string r1)
        {
            var taskIds = new List<int>();
            var taskGroups = new List<string>();

            var entries = r1.Split('\n');
            foreach (var output in entries)
            {
                var groupAndTasks = output.Split(":");
                if (groupAndTasks.Length < 2) continue;

                var groupName = groupAndTasks[0];

                var taskIdsStr = groupAndTasks[1].Split(",").Select(x => x.Trim());
                foreach (var taskIdStr in taskIdsStr)
                {
                    if (int.TryParse(taskIdStr, out var id) && id < dtoOrganiseInputs.tasks.Length)
                    {
                        taskGroups.Add(groupName);
                        taskIds.Add(id);
                    }
                }
            }

            return new DtoOrganiseOutputs() { taskGroups = taskGroups.ToArray(), taskNumbers = taskIds.ToArray() };
        }

        public static DtoAssignRolesOutputs AssignRolesResult(this DtoAssignRolesInputs dtoRolesInputs, string r1)
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

        public static string LinkDependenciesPrompt()
        {
            return "List any dependencies between the tasks (including the existing tasks), one line at a time, in a similar format, using '->'";
        }
    }
}
