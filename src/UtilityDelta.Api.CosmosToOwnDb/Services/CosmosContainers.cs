using Microsoft.Azure.Cosmos;
using System.Collections.ObjectModel;

namespace UtilityDelta.WebAPI.Services
{
    public class CosmosContainers : ICosmosContainers
    {
        private readonly CosmosClient _cosmosClient;

        public Container? ProjectAccess { get; private set; }
        public Container? Events { get; private set; }
        public Container? ShareKeys { get; private set; }

        public CosmosContainers(string endpoint, string key)
        {
            _cosmosClient = new CosmosClient(
                endpoint,
                key,
                new CosmosClientOptions() { AllowBulkExecution = true });
        }

        public async Task Initialise()
        {
            var database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(Constants.COSMOS_DATABASEID);

            ProjectAccess = await InitializeCosmosProjectAccess(database);
            Events = await InitializeCosmosEvents(database);
            ShareKeys = await InitializeCosmosShareKeys(database);
        }

        private static async Task<Container> InitializeCosmosEvents(Database database)
        {
            var containerResponse = await database.CreateContainerIfNotExistsAsync(Constants.COSMOS_CONTAINERID_EVENTS, "/pi");

            containerResponse.AddIndexIfNotExist("/ed/?");

            bool containsComposite = containerResponse.Resource.IndexingPolicy.CompositeIndexes.Count > 0;
            if (!containsComposite)
            {
                containerResponse.Resource.IndexingPolicy.CompositeIndexes.Add(new Collection<CompositePath> {
                    new CompositePath() { Path = "/ed", Order = CompositePathSortOrder.Ascending },
                    new CompositePath() { Path = "/cb", Order = CompositePathSortOrder.Ascending }
                });
            }
            var container = database.GetContainer(Constants.COSMOS_CONTAINERID_EVENTS);
            //await container.ReplaceContainerAsync(containerResponse.Resource);

            return container;
        }

        private static async Task<Container> InitializeCosmosProjectAccess(Database database)
        {
            var containerResponse = await database.CreateContainerIfNotExistsAsync(Constants.COSMOS_CONTAINERID_PROJECTACCESS, "/pi");

            var container = database.GetContainer(Constants.COSMOS_CONTAINERID_PROJECTACCESS);
            //await container.ReplaceContainerAsync(containerResponse.Resource);

            return container;
        }

        private static async Task<Container> InitializeCosmosShareKeys(Database database)
        {
            var containerResponse = await database.CreateContainerIfNotExistsAsync(Constants.COSMOS_CONTAINERID_SHAREKEYS, "/pi");

            var container = database.GetContainer(Constants.COSMOS_CONTAINERID_SHAREKEYS);
            //await container.ReplaceContainerAsync(containerResponse.Resource);

            return container;
        }
    }
}
