using Microsoft.Azure.Cosmos;

namespace UtilityDelta.WebAPI
{
    public interface ICosmosContainers
    {
        Container? ProjectAccess { get; }
        Container? Events { get; }
        Container? ShareKeys { get; }
    }
}
