using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IFileHandlesManager
    {
        bool Exists(string container);
        FileHandles OpenWrite(string container);
    }
}
