using NanoidDotNet;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Services
{
    public class AssistantManager(IOpenAiAssistantCommands openAiAssistantCommands, IAssistantCache assistantCache): IAssistantManager
    {
        public async Task<DtoAssistantChanges> UploadFile(string projectId, string currentUserHash, string system, string fileName, string extension, string iv, Stream document, CancellationToken cancellationToken)
        {
            var events = new List<ProjectEventItem>();
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null)
            {
                assistantId = await openAiAssistantCommands.CreateAssistant(projectId, system);
                events.Add(assistantCache.CreateAssistant(projectId, assistantId, currentUserHash, CancellationToken.None));
            }

            var fileId = await openAiAssistantCommands.UploadFile(Nanoid.Generate() + extension, document, assistantId, cancellationToken);
            events.Add(assistantCache.UploadFile(projectId, assistantId, fileId, fileName, iv, currentUserHash, cancellationToken));

            return new DtoAssistantChanges(events);
        }

        public async Task<DtoAssistantChanges> DeleteFile(string projectId, string currentUserHash, string fileId)
        {
            var events = new List<ProjectEventItem>();
            var assistantId = assistantCache.GetAssistantId(projectId, CancellationToken.None);
            if (assistantId == null) return new DtoAssistantChanges(events);

            var remainingFilesCount = await openAiAssistantCommands.RemoveFileFromAssistant(assistantId, fileId);
            events.Add(assistantCache.RemoveFile(projectId, assistantId, fileId, currentUserHash, CancellationToken.None));

            if (remainingFilesCount != 0)
            {
                return new DtoAssistantChanges(events);
            }

            await openAiAssistantCommands.RemoveAssistant(assistantId);
            events.Add(assistantCache.DeleteAssistant(projectId, assistantId, currentUserHash, CancellationToken.None));
            return new DtoAssistantChanges(events);
        }

        public async Task<DtoAssistantChanges> DeleteAllFiles(string projectId, string currentUserHash)
        {
            var events = new List<ProjectEventItem>();

            var assistantId = assistantCache.GetAssistantId(projectId, CancellationToken.None);
            if (assistantId == null) return new DtoAssistantChanges(events);

            var removedFileIds = await openAiAssistantCommands.RemoveAssistant(assistantId);
            foreach (var fileId in removedFileIds)
            {
                events.Add(assistantCache.RemoveFile(projectId, assistantId, fileId, currentUserHash, CancellationToken.None));
            }
            events.Add(assistantCache.DeleteAssistant(projectId, assistantId, currentUserHash, CancellationToken.None));
            return new DtoAssistantChanges(events);
        }
    }
}
