using Microsoft.Extensions.Options;
using OpenAI;
using OpenAI.Assistants;
using OpenAI.Files;
using System.Threading;
using UtilityDelta.AiTooling.Interfaces;

#pragma warning disable OPENAI001

namespace UtilityDelta.AiTooling.Services
{
    public class OpenAiAssistantCommands(IOptions<ConfigurationEntry> options): IOpenAiAssistantCommands
    {
        public async Task<string> CreateAssistant(string pi, string system)
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);
            var assistantClient = openAIClient.GetAssistantClient();

            AssistantCreationOptions assistantOptions = new()
            {
                Name = pi,
                Instructions = system,
                Tools =
                {
                    new FileSearchToolDefinition()
                },
                ToolResources = new()
                {
                    FileSearch = new()
                    {
                        NewVectorStores =
                        {
                            new VectorStoreCreationHelper(), //Important that we create a vector store immediately
                        }
                    }
                },
            };

            return (await assistantClient.CreateAssistantAsync(options.Value.LLM_MODEL, assistantOptions)).Value.Id;
        }

        public async Task<List<string>> RemoveAssistant(string assistantId)
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);

            var assistantClient = openAIClient.GetAssistantClient();
            var fileClient = openAIClient.GetOpenAIFileClient();
            var vectorStoreClient = openAIClient.GetVectorStoreClient();

            var assistant = await assistantClient.GetAssistantAsync(assistantId, CancellationToken.None);

            var removedFileIds = new List<string>();
            foreach (var vectorStoreId in assistant.Value.ToolResources.FileSearch.VectorStoreIds)
            {
                //How to iterate over the files in the vector store
                var vectorStoreFiles = vectorStoreClient.GetFileAssociationsAsync(vectorStoreId);

                await foreach (var vectorStoreFile in vectorStoreFiles)
                {
                    try
                    {
                        await fileClient.DeleteFileAsync(vectorStoreFile.FileId);  // Deletes the file
                    }
                    catch
                    {
                        //File doesn't exist for some reason, just remove the link
                        await vectorStoreClient.RemoveFileFromStoreAsync(vectorStoreId, vectorStoreFile.FileId, CancellationToken.None);
                    }
                    removedFileIds.Add(vectorStoreFile.FileId);
                }
            }

            await assistantClient.DeleteAssistantAsync(assistantId);

            return removedFileIds;
        }

        public async Task<int> RemoveFileFromAssistant(string assistantId, string fileId)
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);

            var assistantClient = openAIClient.GetAssistantClient();
            var fileClient = openAIClient.GetOpenAIFileClient();
            var vectorStoreClient = openAIClient.GetVectorStoreClient();

            var assistant = await assistantClient.GetAssistantAsync(assistantId, CancellationToken.None);

            var count = 0;
            foreach (var vectorStoreId in assistant.Value.ToolResources.FileSearch.VectorStoreIds)
            {
                //How to iterate over the files in the vector store
                var vectorStoreFiles = vectorStoreClient.GetFileAssociationsAsync(vectorStoreId);

                await foreach (var vectorStoreFile in vectorStoreFiles)
                {
                    if (vectorStoreFile.FileId != fileId)
                    {
                        count++;
                        continue;
                    }
                    await fileClient.DeleteFileAsync(vectorStoreFile.FileId);  // Deletes the file
                }
            }
            return count;
        }

        public async Task<string> UploadFile(string fileName, Stream document, string assistantId, CancellationToken cancellationToken)
        {
            OpenAIClient openAIClient = new(options.Value.OPENAI_API_KEY);

            var assistantClient = openAIClient.GetAssistantClient();
            var vectorStoreClient = openAIClient.GetVectorStoreClient();
            var fileClient = openAIClient.GetOpenAIFileClient();

            var assistant = await assistantClient.GetAssistantAsync(assistantId, cancellationToken);

            var uploadedFile = fileClient.UploadFile(
                document,
                fileName,
                FileUploadPurpose.Assistants,
                cancellationToken);

            var fileId = uploadedFile.Value.Id;
            var vectorStoreId = assistant.Value.ToolResources.FileSearch.VectorStoreIds.First();

            //How to attach the fileId to an existing vectorStoreId under an assistant?
            await vectorStoreClient.AddFileToVectorStoreAsync(vectorStoreId, fileId, false, cancellationToken);

            return fileId;
        }
    }
}