using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IAssistantManager
    {
        Task<DtoAssistantChanges> DeleteAllFiles(string projectId, string currentUserHash);
        Task<DtoAssistantChanges> DeleteFile(string projectId, string currentUserHash, string fileId);
        Task<DtoAssistantChanges> UploadFile(string projectId, string currentUserHash, string system, string fileName, string extension, string iv, Stream document, CancellationToken cancellationToken);
    }
}
