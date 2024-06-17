using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.Schemas;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using UtilityDelta.WebAPI.Data;
using UtilityDelta.WebAPI.Dto;
using UtilityDelta.WebAPI.Entities;

namespace UtilityDelta.WebAPI.Services
{
    public class CosmosQueries
    {
        private readonly ICosmosContainers _cosmosContainers;

        public CosmosQueries(ICosmosContainers cosmosContainers)
        {
            _cosmosContainers = cosmosContainers;
        }

        public async Task<List<ProjectEventItem>> GetEventsAll(string pi)
        {
            var partitionKey = new Microsoft.Azure.Cosmos.PartitionKey(pi);

            var queryDefinition =
                new QueryDefinition("SELECT * FROM c ORDER BY c.ed");

            var queryResultSetIterator = _cosmosContainers.Events!.GetItemQueryIterator<ProjectEventItem>(
                queryDefinition,
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = partitionKey
                });

            var result = new List<ProjectEventItem>();
            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var projectEvent in currentResultSet)
                {
                    result.Add(projectEvent);
                }
            }

            return result;
        }

        public async Task<List<ProjectAccessItem>> GetAllProjectAccess(string pi)
        {
            var partitionKey = new Microsoft.Azure.Cosmos.PartitionKey(pi);

            var queryDefinition =
                new QueryDefinition("SELECT * FROM c ORDER BY c._ts");

            var queryResultSetIterator = _cosmosContainers.ProjectAccess!.GetItemQueryIterator<ProjectAccessItem>(
                queryDefinition,
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = partitionKey
                });

            var result = new List<ProjectAccessItem>();
            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var projectEvent in currentResultSet)
                {
                    result.Add(projectEvent);
                }
            }

            return result;
        }

        public async Task<List<AccessLinkItem>> GetAllShareLinks(string pi)
        {
            var partitionKey = new Microsoft.Azure.Cosmos.PartitionKey(pi);

            var queryDefinition =
                new QueryDefinition("SELECT * FROM c ORDER BY c._ts");

            var queryResultSetIterator = _cosmosContainers.ShareKeys!.GetItemQueryIterator<AccessLinkItem>(
                queryDefinition,
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = partitionKey
                });

            var result = new List<AccessLinkItem>();
            while (queryResultSetIterator.HasMoreResults)
            {
                var currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var projectEvent in currentResultSet)
                {
                    result.Add(projectEvent);
                }
            }

            return result;
        }

        public async Task<List<string>> AllProjects()
        {
            var query = "SELECT DISTINCT c.pi FROM c";
            var queryDefinition = new QueryDefinition(query);
            var partitionKeyValues = new List<string>();

            var queryResultSetIterator = _cosmosContainers.ProjectAccess!.GetItemQueryIterator<dynamic>(queryDefinition);

            while (queryResultSetIterator.HasMoreResults)
            {
                var response = await queryResultSetIterator.ReadNextAsync();
                foreach (var item in response)
                {
                    partitionKeyValues.Add(item.pi.ToString());
                }
            }

            return partitionKeyValues;
        }
    }
}
