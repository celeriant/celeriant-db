using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IFileHandlesManager
    {
        bool Exists(string container);
        FileHandles OpenWrite(string container);
        void Delete(string container);
    }
}
