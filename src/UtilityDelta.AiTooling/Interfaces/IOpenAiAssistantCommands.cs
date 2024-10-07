namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IOpenAiAssistantCommands
    {
        Task<string> CreateAssistant(string pi, string system);
        Task<List<string>> RemoveAssistant(string assistantId);
        Task<int> RemoveFileFromAssistant(string assistantId, string fileId);
        Task<string> UploadFile(string fileName, Stream document, string assistantId, CancellationToken cancellationToken);
        Task<string> UploadFileIndependant(string fileName, Stream document, CancellationToken cancellationToken);
        Task RemoveFileIndependant(string fileId);
    }
}
