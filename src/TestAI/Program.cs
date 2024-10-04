using OpenAI.Assistants;
using OpenAI;
using OpenAI.Chat;
using OpenAI.Files;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using static TestAI.Program;
using System.ClientModel;
using System.ClientModel.Primitives;
using System.Text.RegularExpressions;

namespace TestAI
{
    [JsonSerializable(typeof(TestObj))]
    public partial class ReadSerializerContext : JsonSerializerContext
    {

    }
    public class TestObj
    {
        public int Value1 { get; set; }
        public string Value2 { get; set; }
    }

    internal class Program
    {

        static async Task Main(string[] args)
        {
            var key = Environment.GetEnvironmentVariable("OPENAI_API_KEY")!;
            var documentationAssistantId = "OPENAI_ASSISTANT_ID_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD";

            // Assistants is a beta API and subject to change; acknowledge its experimental status by suppressing the matching warning.
#pragma warning disable OPENAI001
            OpenAIClient openAIClient = new(key);
            var assistantClient = openAIClient.GetAssistantClient();

            await StreamedThread(documentationAssistantId, assistantClient, CancellationToken.None);
        }

        private static async Task StreamedThread(string assistantId, AssistantClient client, CancellationToken cancellationToken)
        {
            var assistant = await client.GetAssistantAsync(assistantId, cancellationToken);

            //Here we lookup the existing threadId for the requesting user
            AssistantThread? thread = null;
            //AssistantThread? thread = await client.GetThreadAsync("thread_ppN1QNzjhHsgPs483q4uSYKO");

            try
            {
                while (true)
                {
                    Console.WriteLine();
                    Console.WriteLine("ENTER PROMPT:");
                    Console.WriteLine();
                    var userQuestion = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(userQuestion) || userQuestion.Trim().ToLower() == "quit") break;

                    if (thread == null) thread = await client.CreateThreadAsync();
                    Console.WriteLine(thread.Id);

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
                                Console.Write(contentUpdate.Text.Replace("\n", Environment.NewLine));
                            }
                        }
                    }
                    while (currentRun?.Status.IsTerminal == false);
                    Console.WriteLine();
                    Console.WriteLine();
                }
            }
            finally
            {
                //Remember to delete the thread when no longer required
                if (thread != null)
                {
                    RequestOptions noThrowOptions = new() { ErrorOptions = ClientErrorBehaviors.NoThrow };
                    _ = await client.DeleteThreadAsync(thread.Id, noThrowOptions);
                }
            }
        }

         private static async Task NonStreamedThread(string documentationAssistantId, AssistantClient assistantClient)
        {
            string? threadId = null;

            while (true)
            {
                Console.WriteLine("Ask a question!");
                var initialQuestion = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(initialQuestion) || initialQuestion.Trim().ToLower() == "quit") break;

                ThreadCreationOptions threadOptions = new()
                {
                    InitialMessages = { initialQuestion }
                };


                ThreadRun threadRun;
                if (threadId == null)
                {
                    threadRun = await assistantClient.CreateThreadAndRunAsync(documentationAssistantId, threadOptions);
                    threadId = threadRun.ThreadId;
                }
                else
                {
                    var rco = new RunCreationOptions();
                    rco.AdditionalMessages.Add(new ThreadInitializationMessage(MessageRole.User, [MessageContent.FromText(initialQuestion)]));
                    threadRun = await assistantClient.CreateRunAsync(threadId, documentationAssistantId, rco);
                }

                do
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    threadRun = await assistantClient.GetRunAsync(threadRun.ThreadId, threadRun.Id);
                } while (!threadRun.Status.IsTerminal);

                var messagePages = assistantClient.GetMessagesAsync(threadRun.ThreadId, new MessageCollectionOptions() { Order = ListOrder.OldestFirst });
                await foreach (ThreadMessage message in messagePages.GetAllValuesAsync())
                {
                    foreach (MessageContent contentItem in message.Content)
                    {
                        if (string.IsNullOrEmpty(contentItem.Text)) continue;
                        Console.WriteLine($"{contentItem.Text}");
                    }
                }
            }


            //Remember to delete the thread when no longer required
            if (threadId != null)
            {
                _ = await assistantClient.DeleteThreadAsync(threadId);
            }
        }
    }
}
