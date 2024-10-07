using OpenAI.Assistants;
using System.Runtime.CompilerServices;

#pragma warning disable OPENAI001

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IUtilityDeltaAssistant
    {
        Task<(string response, string? threadId)> AskAssistantNoStreaming(string assistantId, string? threadId, bool deleteThread, string currentUserHash, List<MessageContent> userQuestion, CancellationToken cancellationToken);
#pragma warning disable CS8424
        IAsyncEnumerable<string> AskAssistant(string? assistantId, bool closeThread, string currentUserHash, string userQuestion, [EnumeratorCancellation] CancellationToken cancellationToken);
#pragma warning restore CS8424
        Task CloseThread(string currentUserHash, string assistantId);
    }
}
