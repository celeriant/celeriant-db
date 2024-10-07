using Microsoft.Extensions.Options;
using OpenAI;
using OpenAI.Assistants;
using OpenAI.Files;
using System.ClientModel;
using System.ClientModel.Primitives;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using UtilityDelta.AiTooling.Interfaces;

#pragma warning disable OPENAI001

namespace UtilityDelta.AiTooling.Services
{
    public class UtilityDeltaAssistant(IOptions<ConfigurationEntry> options) : IUtilityDeltaAssistant
    {
        private ConcurrentDictionary<string, string> _userToThreadIds = new ConcurrentDictionary<string, string>();

        public async Task<(string response, string? threadId)> AskAssistantNoStreaming(string? assistantId, string? threadId, bool deleteThread, string currentUserHash, List<MessageContent> userQuestion, CancellationToken cancellationToken)
        {
            var (assistantClient, assistant) = await GetAssistantClient(assistantId);

            if (threadId == null)
            {
                threadId = (await assistantClient.CreateThreadAsync(new ThreadCreationOptions(), cancellationToken)).Value.Id;
            }

            var threadGet = await assistantClient.GetThreadAsync(threadId, cancellationToken);
            var runCreationOptions = new RunCreationOptions();
            runCreationOptions.AdditionalMessages.Add(new ThreadInitializationMessage(MessageRole.User, userQuestion));
            var threadRun = (await assistantClient.CreateRunAsync(threadId, assistantId, runCreationOptions, cancellationToken)).Value;

            do
            {
                await Task.Delay(1000);
                threadRun = assistantClient.GetRun(threadRun.ThreadId, threadRun.Id);
            } while (!threadRun.Status.IsTerminal);

            CollectionResult<ThreadMessage> messages = assistantClient.GetMessages(threadRun.ThreadId, new MessageCollectionOptions() { Order = MessageCollectionOrder.Ascending }, cancellationToken);

            var result = new StringBuilder();
            foreach (var message in messages)
            {
                if (message.Role != MessageRole.Assistant) continue;

                foreach (MessageContent contentItem in message.Content)
                {
                    if (string.IsNullOrEmpty(contentItem.Text)) continue;

                    // Regular expression to match and remove citation patterns like  
                    string pattern = @"【.*?】";

                    // Replace the citation pattern with an empty string
                    var removedCitations = Regex.Replace(contentItem.Text, pattern, string.Empty);

                    result.AppendLine(removedCitations);
                }
            }

            if (deleteThread)
            {
                await CloseThread(currentUserHash, assistantId);
            }

            return (response: result.ToString(), threadId: (deleteThread ? null : threadRun.ThreadId));
        }

        public async IAsyncEnumerable<string> AskAssistant(string? assistantId, bool closeThread, string currentUserHash, string userQuestion, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(userQuestion)) yield break;

            var (client, assistant) = await GetAssistantClient(assistantId);

            var hashCheck = assistant.Id + currentUserHash;

            //Here we lookup the existing threadId for the requesting user
            AssistantThread thread;
            if (!_userToThreadIds.TryGetValue(hashCheck, out var threadId))
            {
                thread = await client.CreateThreadAsync(cancellationToken: cancellationToken);
                _userToThreadIds.AddOrUpdate(hashCheck, thread.Id, (_, threadId) => thread.Id);
            } else
            {
                thread = await client.GetThreadAsync(threadId, cancellationToken);
            }

            var streamingUpdates = client.CreateRunStreamingAsync(
                thread.Id,
                assistant.Id,
                new RunCreationOptions()
                {
                    AdditionalInstructions = userQuestion,
                }, cancellationToken);

            await foreach (StreamingUpdate streamingUpdate in streamingUpdates)
            {
                if (streamingUpdate is MessageContentUpdate contentUpdate)
                {
                    yield return contentUpdate.Text;
                }
            }

            if (closeThread)
            {
                await CloseThread(currentUserHash, assistant.Id);
            }
        }

        public async Task CloseThread(string currentUserHash, string assistantId)
        {
            var hashCheck = assistantId + currentUserHash;
            if (_userToThreadIds.TryRemove(hashCheck, out var threadId))
            {
                var (client, _) = await GetAssistantClient(assistantId);

                RequestOptions noThrowOptions = new() { ErrorOptions = ClientErrorBehaviors.NoThrow };
                _ = await client.DeleteThreadAsync(threadId, noThrowOptions);
            }
        }

        private async Task<(AssistantClient, Assistant)> GetAssistantClient(string? assistantId)
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);
            var client = openAIClient.GetAssistantClient();
            var assistant = await client.GetAssistantAsync(assistantId ?? options.Value.UD_ASSISTANT_ID);
            return (client, assistant);
        }
    }
}
