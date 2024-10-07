using OpenAI.Assistants;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.Projects.Interfaces;

namespace UtilityDelta.AiTooling.Services
{
    public class SelectLLMProvider(IOpenAiAssistantCommands openAiAssistantCommands, IAssistantLlmProcessing assistantLlmProcessing, ILlmProcessing llmProcessing, IAssistantCache assistantCache) : ISelectLLMProvider
    {
        public async Task<DtoAssignRolesOutputs> AssignRoles(string projectId, string currentUserHash, DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.AssignRoles(dtoRolesInputs, cancellationToken);

            return await assistantLlmProcessing.AssignRoles(assistantId, currentUserHash, dtoRolesInputs, cancellationToken);
        }

        public async Task<DtoBreakdownQuestionsOutputs> BreakdownQuestions(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.BreakdownQuestions(dtoBreakdownInputs, cancellationToken);

            return await assistantLlmProcessing.BreakdownQuestions(assistantId, currentUserHash, dtoBreakdownInputs, cancellationToken);
        }

        public async Task<DtoBreakdownOutputs> ImageBreakdownTask(string projectId, string currentUserHash, string fileName, Stream image, DtoImageBreakdownInputs dtoImageBreakdownInputs, CancellationToken cancellationToken)
        {
            string? toRemoveAssistantId = null;
            string? toRemoveFileId = null;

            try
            {
                var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
                if (assistantId == null)
                {
                    assistantId = await openAiAssistantCommands.CreateAssistant(projectId, dtoImageBreakdownInputs.system ?? "");
                    toRemoveAssistantId = assistantId;
                }

                toRemoveFileId = await openAiAssistantCommands.UploadFileIndependant(fileName, image, cancellationToken);
                dtoImageBreakdownInputs.fileId = toRemoveFileId;
                return await assistantLlmProcessing.ImageBreakdownTask(assistantId, currentUserHash, dtoImageBreakdownInputs, cancellationToken);
            }
            finally
            {
                if (toRemoveFileId != null)
                {
                    await openAiAssistantCommands.RemoveFileIndependant(toRemoveFileId);
                }
                if (toRemoveAssistantId != null)
                {
                    await openAiAssistantCommands.RemoveAssistant(toRemoveAssistantId);
                }
            }
        }

        public async Task<DtoBreakdownOutputs> BreakdownTask(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.BreakdownTask(dtoBreakdownInputs, cancellationToken);
            
            return await assistantLlmProcessing.BreakdownTask(assistantId, currentUserHash, dtoBreakdownInputs, cancellationToken);
        }

        public async Task<DtoRolesOutputs> DetermineRoles(string projectId, string currentUserHash, DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.DetermineRoles(dtoRolesInputs, cancellationToken);

            return await assistantLlmProcessing.DetermineRoles(assistantId, currentUserHash, dtoRolesInputs, cancellationToken);
        }

        public async Task<DtoOrganiseOutputs> GroupTasks(string projectId, string currentUserHash, DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.GroupTasks(dtoOrganiseInputs, cancellationToken);

            return await assistantLlmProcessing.GroupTasks(assistantId, currentUserHash, dtoOrganiseInputs, cancellationToken);
        }

        public async Task<DtoUnknownOutputs> IdentifyUnknowns(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var assistantId = assistantCache.GetAssistantId(projectId, cancellationToken);
            if (assistantId == null) return await llmProcessing.IdentifyUnknowns(dtoBreakdownInputs, cancellationToken);

            return await assistantLlmProcessing.IdentifyUnknowns(assistantId, currentUserHash, dtoBreakdownInputs, cancellationToken);
        }
    }
}
