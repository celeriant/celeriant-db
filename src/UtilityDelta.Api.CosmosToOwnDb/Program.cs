using Microsoft.Extensions.Options;
using Moq;
using System.Globalization;
using System.Linq;
using UtilityDelta.Api.Services;
using UtilityDelta.Api.Shared;
using UtilityDelta.WebAPI.Services;

namespace UtilityDelta.Api.CosmosToOwnDb
{
    internal class Program
    {
        //private const string COSMOS_ENDPOINT = "https://localhost:8081";
        //private const string COSMOS_KEY = "AZURE_COSMOS_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD";

        private static Dictionary<string, string> MY_PROJECTS = new Dictionary<string, string>()
        {
            { "04qI_75MIr8iql-U39TdM", "UtilityDelta Bugs" },
            { "0zHjFWwvME19ss_ru9_Lo", "Example" },
            { "67OJ7rtfDMeFrUSI3FxcK", "Family" },
            { "7XBZFsekfzG02B7WC9Qup", "Magnetica Tasks" },
            { "DdSQ6SfDlwoIsJNQMjM28", "Human-Caused Climate Change" },
            { "E7w_hgE4-qGlFk7OjYm5f", "DAGs" },
            { "Fj6E2ja_kNjV2WL7whX9S", "Personal Today" },
            { "GbTJxP2LNWbJ8mTbXkYVY", "Climate Change" },
            { "KaLeQfhJT7fTP2oxgg0I2", "[Retired] MRI Physics" },
            { "RZ2AeK2YSknQbbfvGDpo5", "UtilityDelta" },
            { "TFiAZX5_amZjnmGACAkeS", "Test" },
            { "W655hYqyf0Ywzjez8LzQ9", "OnPrem UtilityDelta" },
            { "YEAT0bd-EapwBWLgibtF-", "UtilityDelta Internal" },
            { "as_KYo8yvuCJSgUXoFV4M", "Pinnsar" },
            { "cZvNBypVzIC53JAZEd2QY", "RedactedClient" },
            { "jENWbmQ5v8C9nWmUR3qIR", "Old Roadmap" },
            { "t6vNhL_cFHlJxX965eQU2", "Magnetica Clinical App" },
            { "wUTe5H89NkpqinB4an57Z", "UtilityDelta Roadmap" },
            { "wcGMumC6PTutZnQhOU8Nb", "Tyson's Day" },
            //No symmetric keys for these
            { "-h2ElxpLOuxFuF2gWP0yM", "UNKNOWN 1" },
            { "gYC70mSxdc9NJHeD6WKw6", "UNKNOWN 2" },
            { "RNT6_j9hDGz9FhOCuy9LL", "Joel's List" },
            { "WkQIBwEWPeG7LTxQAOsKp", "EMPTY 1" }, //Empty, unused
            { "RpRsLrcvqCiMpds5Wu7nh", "EMPTY 2" }, //Empty, unused
            { "yPdYCpjlFLMjykepbWaPh", "UNKNOWN 6" },
            { "jj9aVATJKMEMdRROV_fHD", "UNKNOWN 7" },
        };

        private const string COSMOS_ENDPOINT = "https://au-utilitydelta.documents.azure.com:443";
        private const string COSMOS_KEY = "AZURE_COSMOS_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD";

        static async Task Main(string[] args)
        {
            var cosmosContainers = new CosmosContainers(COSMOS_ENDPOINT, COSMOS_KEY);
            await cosmosContainers.Initialise();

            var serviceQueries = new CosmosQueries(cosmosContainers);

            var projects = await serviceQueries.AllProjects();

            var createdByMe = new HashSet<string>();
            var buckets = new List<ProjectBucket>();

            foreach (var project in projects)
            {
                var allEvents = await serviceQueries.GetEventsAll(project);
                var allProjectAccess = await serviceQueries.GetAllProjectAccess(project);
                var allShareLinks = await serviceQueries.GetAllShareLinks(project);
                var bucket = new ProjectBucket(project, allEvents, allProjectAccess, allShareLinks);
                buckets.Add(bucket);

                //if (MY_PROJECTS.ContainsKey(project))
                //{
                //    if (!createdByMe.Contains(allProjectAccess[0].id)) createdByMe.Add(allProjectAccess[0].id);

                //    Console.WriteLine($"This is one of my projects: {MY_PROJECTS[project]}");
                //}

                ////if (allEvents.Count < 9) continue;

                //Console.WriteLine($"Processing project: {project}");
                //Console.WriteLine($"Project contains {allEvents.Count} events.");
                //Console.WriteLine($"Project contains {allProjectAccess.Count} project access entries and is created by: {allProjectAccess[0].id}.");
                //Console.WriteLine($"Project contains {allShareLinks.Count} Share Links.");
                
                ////Console.WriteLine();

                ////if (allShareLinks.Count > 0)
                ////{
                ////    foreach (var item in allShareLinks)
                ////    {
                ////        Console.WriteLine($"Sharekey valid until {item.validUntil} - {(item.singleUse ? "singleUse" : "not singleUse")} - {(item.isOwner ? "isOwner" : "not isOwner")} - {(item.readOnly ? "readOnly" : "not readOnly")}");
                ////        Console.WriteLine($"https://app.utilitydelta.io/project/{project}?shareKey={item.id}");
                ////    }
                ////}

                //Console.WriteLine();
                //Console.WriteLine();
            }

            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 1000,
                SUB_DIR_CONTAINERS = "C:\\containers",
                CACHE_CHECK_TIME_HOURS = 1,
                CACHE_MAX_PROJECT_COUNT = 1000,
                CACHE_MAX_SHARE_LINKS_PER_PROJECT = 1000,
                CACHE_MAX_USERS_PER_PROJECT = 1000
            });
            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var writeEvents = new WriteEvents(fileHandlesManager);

            foreach (var bucket in buckets)
            {
                //Add events
                writeEvents.CustomWriteEvents(bucket.events.Select(x => new UtilityDelta.Api.Shared.ProjectEventItem(0, x.cb, x.ed, x.iv, (UtilityDelta.Api.Shared.ProjectEventType)x.tp, x.t1, x.t2, x.t3, x.n1)).ToArray(), bucket.pi, CancellationToken.None);

                //Add share links
                writeEvents.CustomWriteEvents(bucket.accessLinks.Select(x => new UtilityDelta.Api.Shared.ProjectEventItem(
                    serverId: 0,
                    cb: null, //Make cb null so all users get this event
                    ed: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    iv: null,
                    tp: x.singleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink,
                    t1: null,
                    t2: (x.isOwner ? AccessLevel.Owner : x.readOnly ? AccessLevel.Viewer : AccessLevel.Contributor).ToString(),
                    t3: x.id.CalculateHash(), //Must hash the share key to store in events
                    n1: x.validUntil > 0 ? x.validUntil : null)).ToArray(), bucket.pi, CancellationToken.None);

                //Add users
                writeEvents.CustomWriteEvents(bucket.accessItems.Select(x => new UtilityDelta.Api.Shared.ProjectEventItem(
                    serverId: 0,
                    cb: null, //Make cb null so all users get this event
                    ed: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    iv: null,
                    tp: ProjectEventType.ProvideAccess,
                    t1: null,
                    t2: x.id, //forUserId
                    t3: x.shareKey.CalculateHash(), //Must hash the share key to store in events
                    n1: (double)(x.isOwner ? AccessLevel.Owner : x.readOnly ? AccessLevel.Viewer : AccessLevel.Contributor))).ToArray(), bucket.pi, CancellationToken.None);
            }
        }
    }

    public record ProjectBucket(string pi, List<UtilityDelta.WebAPI.Entities.ProjectEventItem> events, List<UtilityDelta.WebAPI.Entities.ProjectAccessItem> accessItems, List<UtilityDelta.WebAPI.Entities.AccessLinkItem> accessLinks);
}
