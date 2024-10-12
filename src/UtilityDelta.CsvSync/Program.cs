using CsvHelper;
using Microsoft.Extensions.Logging;
using NanoidDotNet;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;
using static System.Net.Mime.MediaTypeNames;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace UtilityDelta.CsvSync
{
    internal class Program
    {

        public static int HashProjectId(string projectId)
        {
            using (SHA256 sha256 = SHA256.Create())
            {
                byte[] data = Encoding.UTF8.GetBytes(projectId);
                byte[] hashBytes = sha256.ComputeHash(data);
                return hashBytes[0];
            }
        }

        private static string baseUrl()
    {
#if DEBUG
        int hash = HashProjectId(projectId);
        // Using bitwise AND with 1 to check if the hash is even or odd
        return (hash % 2) == 0 ? "https://api2.utilitydelta.io:1001" : "https://api2.utilitydelta.io:1000";
#endif
        return "http://localhost:5198";
    }

        //private static string projectId = "u8e6DG1NRkjJUZkHzLILr";
        //private static string linkBase = "https://saludamedical.atlassian.net/browse/";
        //private static string TLTTitle = "Saluda2";

        private static string projectId = "SB3ldQsj334dDsM19t6ml";
        private static string linkBase = "https://megt.atlassian.net/browse/";
        private static string TLTTitle = "MEGT";

        private static string endpoint = "/api/write";
        private static string endpointRead = "/api/read";
        private static string privateKey = "-----BEGIN PRIVATE KEY-----\nRSA_PRIVATE_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD\n-----END PRIVATE KEY-----";
        private static string publicKey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxntHeB/jchsCY0E9cD/3Jgx88kqgvy2gkwTXjK2SOHL5Qnfcu6bIgqXlYYglnJzWTWZ+6LuM4ghsqo3UN92fDCQ+hCwlrUaF6EzFN1gcprVu+za/KaeRaON0MhEzyOwinFzYuB1geV59qqX1rQR3Uf4G92pioau3/5hYTArotJ0fIc+6PsIxkaOTr/EiOYhWA3AaaMyJzTkUu9meX1FBpMq2TZdD7bE/7BOIoKIY44I7mmDjqXU/3sc2P/bnb8veySGjuXaNGAFt6sYKPFrFojhTHV0NhTMemKOyLSUqIhupU96ZQWSPXHrgA3wA1jV2xWhHxMyxDurh+UEOKA738wIDAQAB\n-----END PUBLIC KEY-----";
        
        private static string symmetricKey = "SYMMETRIC_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD";
        
        private static AesGcm b_symmetricKey = AesGcmEncryption.ImportKeyJwkSymmetric(symmetricKey);

        private static void NonceAndSignIt(RSA assymetric, out string nonce, out string nonceSigned)
        {
            nonce = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            nonceSigned = Convert.ToBase64String(assymetric.SignData(Encoding.UTF8.GetBytes(nonce), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        }

        private static void ExecutePythonScript()
        {
            // Path to the Python executable
            string pythonExePath = @"C:\Users\UtilityDelta\AppData\Local\Programs\Python\Python310\python.exe";
            // Path to the Python script you want to execute
            string scriptPath = @"C:\Users\UtilityDelta\source\repos\pythonplay\accessjira.py";

            // Create the process start info
            var start = new ProcessStartInfo
            {
                FileName = pythonExePath,  // Python executable
                Arguments = scriptPath,    // Python script path
                RedirectStandardOutput = true,  // Capture standard output
                RedirectStandardError = true,   // Capture standard error
                UseShellExecute = false,        // Don't use OS shell
                CreateNoWindow = true           // Don't create a window for the process
            };

            using (Process process = Process.Start(start))
            {
                using (var reader = process.StandardOutput)
                {
                    string result = reader.ReadToEnd();  // Get output from the script
                    Console.WriteLine(result);
                }

                // Read any errors that might have occurred
                using (var reader = process.StandardError)
                {
                    string error = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(error))
                    {
                        Console.WriteLine("Error: " + error);
                    }
                }
            }
        }

        const int IV_LENGTH_BYTES = 12;
        const int ENCRYPTION_KEY_BITS = 128;

        public static async Task<byte[]> GenerateKeysSymmetricAsync()
        {
            using (var aes = Aes.Create())
            {
                aes.KeySize = ENCRYPTION_KEY_BITS; // Set your desired key size in bits (e.g., 128, 192, 256)
                aes.GenerateKey();
                return aes.Key; // Returns the generated key
            }
        }

        private static List<JiraTask> GetJiraTasks(string csvFilePath)
        {
            // List to hold the parsed tasks
            List<JiraTask> tasks = new List<JiraTask>();

            // Read the CSV file using CsvHelper
            using (var reader = new StreamReader(csvFilePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                tasks = new List<JiraTask>(csv.GetRecords<JiraTask>());
            }

            return tasks;
        }

        public static RSA CreateRsaFromPrivateKey(string privateKeyPem)
        {
            // Remove header and footer if present
            const string rsaPrivateKeyHeader = "-----BEGIN PRIVATE KEY-----";
            const string rsaPrivateKeyFooter = "-----END PRIVATE KEY-----";

            if (privateKeyPem.Contains(rsaPrivateKeyHeader))
            {
                privateKeyPem = privateKeyPem.Replace(rsaPrivateKeyHeader, string.Empty)
                                             .Replace(rsaPrivateKeyFooter, string.Empty)
                                             .Trim();
            }

            // Convert from Base64 to byte array
            byte[] privateKeyBytes = Convert.FromBase64String(privateKeyPem);

            // Create RSA object and import private key
            RSA rsa = RSA.Create();
            rsa.ImportPkcs8PrivateKey(privateKeyBytes, out _); // PKCS#8 private key format
            return rsa;
        }
        private static bool requiresEncryption(ProjectEventType type)
        {
            switch (type)
            {
                case ProjectEventType.AddTask:
                case ProjectEventType.SetLink:
                case ProjectEventType.AddRole:
                case ProjectEventType.SetRoleName:
                case ProjectEventType.AddTeamMember:
                case ProjectEventType.SetTeamMemberName:
                case ProjectEventType.AddItemToStandup:
                case ProjectEventType.RetroDiscussionItemAdd:
                case ProjectEventType.SetTaskSummary:
                case ProjectEventType.AddSingleUseShareLink:
                case ProjectEventType.AddShareLink:
                case ProjectEventType.ProvideAccess:
                case ProjectEventType.SetProjectDescription:
                    return true;

                case ProjectEventType.SetParent:
                case ProjectEventType.SetTaskStatus:
                case ProjectEventType.CollapseTask:
                case ProjectEventType.RemoveTask:
                case ProjectEventType.SetDueDate:
                case ProjectEventType.SetAssignedTo:
                case ProjectEventType.SetEstimate:
                case ProjectEventType.UnsetTaskStatus:
                case ProjectEventType.SetConfidence:
                case ProjectEventType.AddPredecessor:
                case ProjectEventType.AddSuccessor:
                case ProjectEventType.BeginStandup:
                case ProjectEventType.RemovePredecessor:
                case ProjectEventType.RemoveSuccessor:
                case ProjectEventType.SetProjectOwner:
                case ProjectEventType.AddProjectMember:
                case ProjectEventType.SetRoleIsActive:
                case ProjectEventType.SetTeamMemberHours:
                case ProjectEventType.AddTeamMemberRoleId:
                case ProjectEventType.RemoveTeamMemberRoleId:
                case ProjectEventType.SetTeamMemberIsActive:
                case ProjectEventType.SetRoleId:
                case ProjectEventType.SetTeamMemberAuthId:
                case ProjectEventType.SetDefaultTaskDuration:
                case ProjectEventType.StandupCompleted:
                case ProjectEventType.StandupItemTime:
                case ProjectEventType.StandupNextItem:
                case ProjectEventType.RetroStart:
                case ProjectEventType.RetroCancel:
                case ProjectEventType.RetroEnd:
                case ProjectEventType.RetroDiscussionItemDelete:
                case ProjectEventType.RetroDiscussionItemGroup:
                case ProjectEventType.RetroMakeVisible:
                case ProjectEventType.RemoveProjectMember:
                case ProjectEventType.DisableShareLink:
                case ProjectEventType.SaveNodePositions:
                    return false;
                default:
                    throw new Exception("Unknown ProjectEventType" + type);
            }
        }

        private static ProjectEventItem DecryptEvent(ProjectEventItem item)
        {
            if (item.iv == null || string.IsNullOrEmpty(item.t3) || !requiresEncryption(item.tp) || item.tp == ProjectEventType.AddShareLink) return item;

            var newT3 = BufferHelper.Ab2StrNotBase64(AesGcmEncryption.DecryptSymmetric(BufferHelper.Str2Ab(item.iv!), b_symmetricKey, BufferHelper.Str2Ab(item.t3!)));

            return new ProjectEventItem(item.serverId, item.cb, item.ed, null, item.tp, item.t1, item.t2, newT3, item.n1);
        }

        public static Dictionary<string, JiraTask> GetCurrentState(List<ProjectEventItem> events)
        {
            var result = new Dictionary<string, JiraTask>();

            events = events.Select(x => DecryptEvent(x)).ToList();

            foreach (var item in events)
            {
                switch (item.tp)
                {
                    case ProjectEventType.AddTask:
                        result.Add(item.t1, new JiraTask()
                        {
                            DateCreated = DateTimeOffset.FromUnixTimeSeconds(item.ed).DateTime,
                            DateLastModified = DateTimeOffset.FromUnixTimeSeconds(item.ed).DateTime,
                            Status = "To Do",
                            ParentTask = item.t2 ?? "No parent",
                            Summary = item.t3!
                        });
                        break;
                    case ProjectEventType.SetParent:
                        result[item.t1!].ParentTask = item.t2!;
                        result[item.t1!].DateLastModified = DateTimeOffset.FromUnixTimeSeconds(item.ed).DateTime;
                        break;
                    case ProjectEventType.SetTaskSummary:
                        result[item.t1!].Summary = item.t3!;
                        result[item.t1!].DateLastModified = DateTimeOffset.FromUnixTimeSeconds(item.ed).DateTime;
                        break;
                    case ProjectEventType.SetTaskStatus:
                        var status = (Projects.Shared.TaskStatus)item.n1!;
                        string statusJira = TaskStatusToJira(status);
                        result[item.t1!].Status = statusJira;
                        result[item.t1!].DateLastModified = DateTimeOffset.FromUnixTimeSeconds(item.ed).DateTime;

                        break;
                        //case ProjectEventType.RemoveTask:
                        //    break;
                        //case ProjectEventType.AddSuccessor:
                        //    break;
                        //case ProjectEventType.RemoveSuccessor:
                        //    break;
                }
            }

            return result;
        }

        private static Projects.Shared.TaskStatus JiraStatusToTaskStatus(string jiraStatus)
        {
            var statusJira = Projects.Shared.TaskStatus.Pending;
            switch (jiraStatus)
            {
                case "Done":
                    statusJira = Projects.Shared.TaskStatus.Completed;
                    break;
                case "Blocked":
                    statusJira = Projects.Shared.TaskStatus.Blocked;
                    break;
                case "In Progress":
                    statusJira = Projects.Shared.TaskStatus.InProgress;
                    break;
                case "To Do":
                    statusJira = Projects.Shared.TaskStatus.Pending;
                    break;
            }

            return statusJira;
        }

        private static string TaskStatusToJira(Projects.Shared.TaskStatus status)
        {
            var statusJira = "To Do";
            switch (status)
            {
                case Projects.Shared.TaskStatus.Completed:
                    statusJira = "Done";
                    break;
                case Projects.Shared.TaskStatus.Blocked:
                    statusJira = "Blocked";
                    break;
                case Projects.Shared.TaskStatus.InProgress:
                    statusJira = "In Progress";
                    break;
                case Projects.Shared.TaskStatus.Pending:
                    statusJira = "To Do";
                    break;
            }

            return statusJira;
        }

        private static async Task<DtoRead> DoRead()
        {
            using var client = new HttpClient();

            var assymetricRSA = CreateRsaFromPrivateKey(privateKey);
            NonceAndSignIt(assymetricRSA, out var nonce, out var nonceSigned);

            var uriBuilder2 = new UriBuilder(baseUrl() + endpointRead);

            var query2 = System.Web.HttpUtility.ParseQueryString(uriBuilder2.Query);
            query2["pi"] = projectId;
            query2["publicKey"] = publicKey;
            query2["nonce"] = nonce;
            query2["sign"] = nonceSigned;
            query2["fromTime"] = "0";
            query2["includeMyEvents"] = "true";
            query2["createIfNotExist"] = "false";
            uriBuilder2.Query = query2.ToString();

            HttpResponseMessage response = await client.GetAsync(uriBuilder2.Uri);
            var json = await response.Content.ReadAsStringAsync();
            try
            {
                var events = System.Text.Json.JsonSerializer.Deserialize<DtoRead>(json);
                return events!;
            } catch
            {
                return new DtoRead(new List<ProjectEventItem>(), 0);
            }
        }

        public static (List<string> NewTasks, List<string> RemovedTasks) GetTaskDifferences(List<string> updated, List<string> existing)
        {
            // Convert arrays to HashSets for fast lookup.
            var updatedSet = new HashSet<string>(updated);
            var existingSet = new HashSet<string>(existing);

            // Find tasks that are in 'updated' but not in 'existing'.
            var newTasks = updatedSet.Except(existingSet).ToList();

            // Find tasks that are in 'existing' but not in 'updated'.
            var removedTasks = existingSet.Except(updatedSet).ToList();

            return (newTasks, removedTasks);
        }

        static void AddDependencies(List<ProjectEventItem> projectEvents, string depStr, string existingDepStr, long eventTime, string taskId)
        {
            var sourceDeps = depStr == "No dependencies" || string.IsNullOrWhiteSpace(depStr) ? new List<string>() : depStr.Split(',').ToList();
            var existingDeps = existingDepStr == "No dependencies" || string.IsNullOrWhiteSpace(existingDepStr) ? new List<string>() : existingDepStr.Split(',').ToList();

            var (addDeps, removeDeps) = GetTaskDifferences(sourceDeps, existingDeps);

            var timeIncrement = 1;
            foreach (var dep in addDeps)
            {
                projectEvents.Add(new ProjectEventItem(0, null,
                    eventTime + timeIncrement, null,
                    ProjectEventType.AddSuccessor,
                    t1: taskId,
                    t2: dep,
                    t3: null,
                    n1: null));

                timeIncrement++;
            }

            foreach (var dep in removeDeps)
            {
                projectEvents.Add(new ProjectEventItem(0, null,
                    eventTime + timeIncrement, null,
                    ProjectEventType.RemoveSuccessor,
                    t1: taskId,
                    t2: dep,
                    t3: null,
                    n1: null));

                timeIncrement++;
            }
        }

        public static int? ExtractTicketNumber(string ticket)
        {
            var match = Regex.Match(ticket, @"\d+");
            return match.Success ? int.Parse(match.Value) : (int?)null;
        }

        public static List<ProjectEventItem> CompareLists(List<JiraTask> source, Dictionary<string, JiraTask> destinationLookup, List<UDTeamMember> teamMembers, bool dateCreatedSort)
        {
            var projectEvents = new List<ProjectEventItem>();

            if (destinationLookup.Count == 0)
            {
                //Create TLT
                projectEvents.Add(new ProjectEventItem(0, null,
                    source.Min(x => x.DateCreated).AddMinutes(-1).ToUniversalTime().ToUnixTimeSeconds(), null,
                    ProjectEventType.AddTask,
                    t1: projectId,
                    t2: null,
                    t3: TLTTitle,
                    n1: null));
            }

            //source = source.OrderBy(x => ExtractTicketNumber(x.TaskId)).ToList();

            if (dateCreatedSort)
            {
                source = source.OrderBy(x => x.DateCreated).ToList();
            }
            else
            {
                source = source.OrderBy(x => x.DateLastModified).ToList();
            }

            var processDependenciesFor = new List<string>();

            // Iterate through the source list
            foreach (var sourceTask in source)
            {
                if (!destinationLookup.TryGetValue(sourceTask.TaskId, out var destinationTask))
                {
                    // If the task does not exist in destination, add it
                    projectEvents.Add(new ProjectEventItem(0, null, 
                        sourceTask.DateCreated.ToUniversalTime().ToUnixTimeSeconds(), null, 
                        ProjectEventType.AddTask,
                        t1: sourceTask.TaskId,
                        t2: projectId, //Always add new tasks directly under the projectid TLT, changing the parent later
                        t3: sourceTask.Summary, 
                        n1: null));

                    projectEvents.Add(new ProjectEventItem(0, null,
                        sourceTask.DateCreated.ToUniversalTime().ToUnixTimeSeconds() + 1, null,
                        ProjectEventType.SetLink,
                        t1: sourceTask.TaskId,
                        t2: null,
                        t3: $"{linkBase}{sourceTask.TaskId}",
                        n1: null));

                    if (sourceTask.AssignedTo != "Unassigned")
                    {
                        ProcessTeamMember(teamMembers, projectEvents, sourceTask, true);
                    }

                    if (sourceTask.Status != "To Do")
                    {
                        projectEvents.Add(new ProjectEventItem(0, null,
                            sourceTask.DateCreated.ToUniversalTime().ToUnixTimeSeconds() + 3, null,
                            ProjectEventType.SetTaskStatus,
                            t1: sourceTask.TaskId,
                            t2: null,
                            t3: null,
                            n1: (int)JiraStatusToTaskStatus(sourceTask.Status)));
                    }
                }
                else
                {
                    // Task exists, compare properties
                    if (sourceTask.Summary != destinationTask.Summary)
                    {
                        projectEvents.Add(new ProjectEventItem(0, null,
                            sourceTask.DateLastModified.ToUniversalTime().ToUnixTimeSeconds(), null,
                            ProjectEventType.SetTaskSummary,
                            t1: sourceTask.TaskId,
                            t2: null,
                            t3: sourceTask.Summary, n1: null));
                    }

                    if (sourceTask.Status != destinationTask.Status)
                    {
                        projectEvents.Add(new ProjectEventItem(0, null,
                            sourceTask.DateLastModified.ToUniversalTime().ToUnixTimeSeconds() + 1, null,
                            ProjectEventType.SetTaskStatus,
                            t1: sourceTask.TaskId,
                            t2: null,
                            t3: null, 
                            n1: (int)JiraStatusToTaskStatus(sourceTask.Status)));
                    }

                    if (sourceTask.AssignedTo != destinationTask.AssignedTo)
                    {
                        ProcessTeamMember(teamMembers, projectEvents, sourceTask);
                    }
                }
            }

            foreach (var sourceTask in source)
            {
                destinationLookup.TryGetValue(sourceTask.TaskId, out var destinationTask);
                if (destinationTask == null) destinationTask = new JiraTask() { ParentTask = "", Dependencies = "" };

                if (destinationTask.ParentTask == "" && sourceTask.ParentTask != "No parent")
                {
                    projectEvents.Add(new ProjectEventItem(0, null,
                        sourceTask.DateLastModified.ToUniversalTime().ToUnixTimeSeconds() + 5, null,
                        ProjectEventType.SetParent,
                        t1: sourceTask.TaskId,
                        t2: sourceTask.ParentTask,
                        t3: null,
                        n1: null));
                }
                else if (destinationTask.ParentTask != "" && sourceTask.ParentTask != destinationTask.ParentTask)
                {
                    projectEvents.Add(new ProjectEventItem(0, null,
                        sourceTask.DateLastModified.ToUniversalTime().ToUnixTimeSeconds() + 5, null,
                        ProjectEventType.SetParent,
                        t1: sourceTask.TaskId,
                        t2: sourceTask.ParentTask == "No parent" ? projectId : sourceTask.ParentTask,
                        t3: null,
                        n1: null));
                }

                AddDependencies(projectEvents, sourceTask.Dependencies, destinationTask.Dependencies, sourceTask.DateLastModified.ToUniversalTime().ToUnixTimeSeconds() + 6, sourceTask.TaskId);

            }

            return projectEvents;
        }

        private static void ProcessTeamMember(List<UDTeamMember> teamMembers, List<ProjectEventItem> projectEvents, JiraTask sourceTask, bool useCreatedDate = false)
        {

            var date = (useCreatedDate ? sourceTask.DateCreated : sourceTask.DateLastModified);

            var existing = teamMembers.FirstOrDefault(x => x.Name == sourceTask.AssignedTo);
            if (existing == null)
            {
                var teamMemberId = Nanoid.Generate();

                projectEvents.Add(new ProjectEventItem(0, null,
                    date.ToUniversalTime().ToUnixTimeSeconds() + 2, null,
                    ProjectEventType.AddTeamMember,
                    t1: teamMemberId,
                    t2: null,
                    t3: sourceTask.AssignedTo,
                    n1: null));

                teamMembers.Add(new UDTeamMember() { Id = teamMemberId, Name = sourceTask.AssignedTo });

                projectEvents.Add(new ProjectEventItem(0, null,
                    date.ToUniversalTime().ToUnixTimeSeconds() + 3, null,
                    ProjectEventType.SetAssignedTo,
                    t1: sourceTask.TaskId,
                    t2: teamMemberId,
                    t3: null,
                    n1: null));
            }
            else
            {
                projectEvents.Add(new ProjectEventItem(0, null,
                    date.ToUniversalTime().ToUnixTimeSeconds() + 3, null,
                    ProjectEventType.SetAssignedTo,
                    t1: sourceTask.TaskId,
                    t2: existing.Id,
                    t3: null,
                    n1: null));
            }
        }

        private static async Task SendViaWebClient(HttpClient client, string nonce, string sign, List<ProjectEventItem> events)
        {
            var uriBuilder = new UriBuilder(baseUrl() + endpoint);
            var query = System.Web.HttpUtility.ParseQueryString(uriBuilder.Query);
            query["pi"] = projectId;
            query["publicKey"] = publicKey;
            query["nonce"] = nonce;
            query["sign"] = sign;
            query["fromTime"] = "0";
            query["createIfNotExist"] = "true";
            uriBuilder.Query = query.ToString();

            var json = JsonSerializer.Serialize(events);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            try
            {
                HttpResponseMessage response = await client.PostAsync(uriBuilder.Uri, content);
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();
                Console.WriteLine("Response received: " + responseBody);
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine("Request error: " + e.Message);
            }
        }

        private static async Task SendViaWeb(List<ProjectEventItem> newEvents)
        {
            using var client = new HttpClient();

            var assymetricRSA = CreateRsaFromPrivateKey(privateKey);
            NonceAndSignIt(assymetricRSA, out var nonce, out var nonceSigned);

            var encryptedEvents = newEvents.Select(eventItem =>
            {
                if (!requiresEncryption(eventItem.tp) || string.IsNullOrEmpty(eventItem.t3)) return eventItem;

                var iv = AesGcmEncryption.CreateIV();
                var b_t3 = AesGcmEncryption.EncryptSymmetric(iv, b_symmetricKey, BufferHelper.Str2AbNotBase64(eventItem.t3));

                return new ProjectEventItem(0, null, eventItem.ed, BufferHelper.Ab2Str(iv), eventItem.tp, eventItem.t1, eventItem.t2, BufferHelper.Ab2Str(b_t3), eventItem.n1);
            }).ToList();

            await SendViaWebClient(client, nonce, nonceSigned, encryptedEvents);
        }

        private static async Task<DtoShare> CreateShareLink()
        {
            using var client = new HttpClient();

            var assymetricRSA = CreateRsaFromPrivateKey(privateKey);
            NonceAndSignIt(assymetricRSA, out var nonce, out var nonceSigned);

            var uriBuilder = new UriBuilder(baseUrl() + "/api/share");
            var query = System.Web.HttpUtility.ParseQueryString(uriBuilder.Query);
            query["pi"] = projectId;
            query["publicKey"] = publicKey;
            query["nonce"] = nonce;
            query["sign"] = nonceSigned;
            query["isOwner"] = "true";
            query["singleUse"] = "false";
            query["expiresOn"] = "0";
            query["readOnly"] = "false";
            uriBuilder.Query = query.ToString();

            HttpResponseMessage response = await client.PostAsync(uriBuilder.Uri, null);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<DtoShare>(json)!;
        }


        public static async Task Main(string[] args)
        {
            ExecutePythonScript();

            // Path to the CSV file
            string csvFilePathCurrent = "jira_tasks.csv";
            string csvFilePathPrevious = $"jira_tasks_previous{TLTTitle}.csv";
            string jsonTeamMembers = "teamMembers.json";

            bool cleanRun = false;

            var currentTasks = GetJiraTasks(csvFilePathCurrent);
            //var task = currentTasks.FirstOrDefault(x => x.TaskId == "LAS-164");

            var currentState = !cleanRun && File.Exists(csvFilePathPrevious) ? GetJiraTasks(csvFilePathPrevious).ToDictionary(x => x.TaskId, x => x) : new Dictionary<string, JiraTask>();
            //var hasTaskCurrent = currentState.ContainsKey(task.TaskId);

            //Reading from UD in case we want to sync state from there instead of rpevious csv
            //var read = await DoRead();
            //var currentStateUD = GetCurrentState(read.events);

            var currentUsers = !cleanRun && File.Exists(jsonTeamMembers) ? 
                JsonSerializer.Deserialize<List<UDTeamMember>>(File.ReadAllText(jsonTeamMembers)) : 
                new List<UDTeamMember>();

            //TODO: Add team members to current state
            //TODO: Add dependencies - test with UD Jira
            //TODO: Deleted tasks list, delete after task > 7 days old, don't re-add them as well.

            var newEvents = CompareLists(currentTasks, currentState, currentUsers!, cleanRun || !File.Exists(csvFilePathPrevious));
            if (newEvents.Count > 0)
            {
                await SendViaWeb(newEvents);
            }

            File.WriteAllText(jsonTeamMembers, JsonSerializer.Serialize(currentUsers));
            File.Copy(csvFilePathCurrent, csvFilePathPrevious, true);

            if (cleanRun)
            {
                var shareLink = await CreateShareLink();
#if DEBUG
                Console.WriteLine($"http://localhost:5173/project/{projectId}?shareKey={shareLink.shareKey}#sk={symmetricKey}");
#else
                Console.WriteLine($"https://app.utilitydelta.io/project/{projectId}?shareKey={shareLink.shareKey}#sk={symmetricKey}");
#endif
                Console.ReadLine();
            }
            
        }
    }


    public class UDTeamMember
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

}

