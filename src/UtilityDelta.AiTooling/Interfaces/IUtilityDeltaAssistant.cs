using System.Runtime.CompilerServices;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IUtilityDeltaAssistant
    {
#pragma warning disable CS8424
        IAsyncEnumerable<string> AskAssistant(string currentUserHash, string userQuestion, [EnumeratorCancellation] CancellationToken cancellationToken);
#pragma warning restore CS8424
        Task CloseThread(string currentUserHash);
    }
}
