using Microsoft.Extensions.Options;
using OpenAI;
using OpenAI.Assistants;
using System.ClientModel.Primitives;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using UtilityDelta.AiTooling.Interfaces;

#pragma warning disable OPENAI001

namespace UtilityDelta.AiTooling.Services
{
    public class UtilityDeltaAssistant(IOptions<ConfigurationEntry> options) : IUtilityDeltaAssistant
    {
        private ConcurrentDictionary<string, string> _userToThreadIds = new ConcurrentDictionary<string, string>();

        public async IAsyncEnumerable<string> AskAssistant(string currentUserHash, string userQuestion, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(userQuestion)) yield break;

            var (client, assistant) = await GetAssistantClient();

            //Here we lookup the existing threadId for the requesting user
            AssistantThread thread;
            if (!_userToThreadIds.TryGetValue(currentUserHash, out var threadId))
            {
                thread = await client.CreateThreadAsync(cancellationToken: cancellationToken);
                _userToThreadIds.AddOrUpdate(currentUserHash, thread.Id, (_, threadId) => thread.Id);
            } else
            {
                thread = await client.GetThreadAsync(threadId, cancellationToken);
            }

            var message = await client.CreateMessageAsync(thread, MessageRole.User, [userQuestion]);

            var asyncUpdates = client.CreateRunStreamingAsync(thread, assistant);
            ThreadRun? currentRun = null;
            do
            {
                currentRun = null;
                await foreach (StreamingUpdate update in asyncUpdates)
                {
                    if (update is RunUpdate runUpdate)
                    {
                        currentRun = runUpdate;
                    }
                    else if (update is MessageContentUpdate contentUpdate && !string.IsNullOrWhiteSpace(contentUpdate.Text))
                    {
                        yield return contentUpdate.Text;
                    }
                }
            }
            while (currentRun?.Status.IsTerminal == false);
        }

        public async Task CloseThread(string currentUserHash)
        {
            if (_userToThreadIds.TryRemove(currentUserHash, out var threadId))
            {
                var (client, _) = await GetAssistantClient();

                RequestOptions noThrowOptions = new() { ErrorOptions = ClientErrorBehaviors.NoThrow };
                _ = await client.DeleteThreadAsync(threadId, noThrowOptions);
            }
        }

        private async Task<(AssistantClient, Assistant)> GetAssistantClient()
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);
            var client = openAIClient.GetAssistantClient();
            var assistant = await client.GetAssistantAsync(options.Value.UD_ASSISTANT_ID);
            return (client, assistant);
        }
    }
}
