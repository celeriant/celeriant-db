using OpenAI.Assistants;
using OpenAI.Chat;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;

#pragma warning disable OPENAI001

namespace UtilityDelta.AiTooling.Services
{
    public class AssistantLlmProcessing(IUtilityDeltaAssistant utilityDeltaAssistant) : IAssistantLlmProcessing
    {

        public async Task<DtoBreakdownOutputs> ImageBreakdownTask(string assistantId, string currentUserHash, DtoImageBreakdownInputs dtoImageBreakdownInputs, CancellationToken cancellationToken)
        {
            var u1 = MessageContent.FromText(dtoImageBreakdownInputs.ImageBreakdownPrompt());
            var f1 = MessageContent.FromImageFileId(dtoImageBreakdownInputs.fileId);

            var (result, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(assistantId, null, true, currentUserHash, [ u1, f1 ], cancellationToken);
            
            return dtoImageBreakdownInputs.ImageBreakdownResult(result);
        }

        public async Task<DtoAssignRolesOutputs> AssignRoles(string projectId, string currentUserHash, DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoRolesInputs.AssignRolesPrompt();

            var (result,_) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, true, currentUserHash, [prompt], cancellationToken);

            return dtoRolesInputs.AssignRolesResult(result);
        }

        public async Task<DtoBreakdownQuestionsOutputs> BreakdownQuestions(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoBreakdownInputs.AutoBreakdownInitialQuestionsPrompt(true);
            var (result, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, true, currentUserHash, [prompt], cancellationToken);
            return dtoBreakdownInputs.AutoBreakdownInitialQuestionsResult(result);
        }

        public async Task<DtoBreakdownOutputs> BreakdownTask(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var prompt1 = dtoBreakdownInputs.AutoBreakdownInitialPrompt(true);
            var (result1, threadId) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, false, currentUserHash, [prompt1], cancellationToken);

            if (dtoBreakdownInputs.skipDependencies)
            {
                return dtoBreakdownInputs.AutoBreakdownResult(result1, string.Empty);
            }

            var prompt2 = PromptEngineering.LinkDependenciesPrompt();
            var (result2, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, threadId, true, currentUserHash, [prompt2], cancellationToken);
            return dtoBreakdownInputs.AutoBreakdownResult(result1, result2);
        }

        public async Task<DtoRolesOutputs> DetermineRoles(string projectId, string currentUserHash, DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoRolesInputs.DetermineRolesPrompt();

            var (result, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, true, currentUserHash, [prompt], cancellationToken);

            return PromptEngineering.BuildRolesResult(result);
        }

        public async Task<DtoOrganiseOutputs> GroupTasks(string projectId, string currentUserHash, DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoOrganiseInputs.GroupTasksPrompt();

            var (result, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, true, currentUserHash, [prompt], cancellationToken);

            return dtoOrganiseInputs.GroupTasksResult(result);
        }

        public async Task<DtoUnknownOutputs> IdentifyUnknowns(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoBreakdownInputs.DiscoverUnknownsPrompt();

            var (result, _) = await utilityDeltaAssistant.AskAssistantNoStreaming(projectId, null, true, currentUserHash, [prompt], cancellationToken);

            return dtoBreakdownInputs.BuildUnknownResult(result);
        }
    }
}
