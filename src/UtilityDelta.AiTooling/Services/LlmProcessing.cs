using Microsoft.Extensions.Options;
using OpenAI.Chat;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;

namespace UtilityDelta.AiTooling.Services
{
    public class LlmProcessing(IOptions<ConfigurationEntry> config) : ILlmProcessing
    {
        public async Task<DtoAssignRolesOutputs> AssignRoles(DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoRolesInputs.AssignRolesPrompt();

            var result = await SingleShotChat(prompt, cancellationToken);

            return dtoRolesInputs.AssignRolesResult(result);
        }

        private async Task<string> SingleShotChat(string prompt, CancellationToken cancellationToken)
        {
            return (await NewChat().CompleteChatAsync([prompt], cancellationToken: cancellationToken)).Value.Content[0].Text;
        }

        private ChatClient NewChat()
        {
            return new ChatClient(model: config.Value.LLM_MODEL, config.Value.OPENAI_API_KEY);
        }

        public async Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var chat = NewChat();

            var u1 = ChatMessage.CreateUserMessage(dtoBreakdownInputs.AutoBreakdownInitialPrompt());

            var r1 = await chat.CompleteChatAsync([u1], cancellationToken: cancellationToken);
            var a1 = ChatMessage.CreateAssistantMessage(r1);

            var u2 = ChatMessage.CreateUserMessage(PromptEngineering.LinkDependenciesPrompt());

            var r2 = await chat.CompleteChatAsync([u1, a1, u2], cancellationToken: cancellationToken);

            return dtoBreakdownInputs.AutoBreakdownResult(r1.Value.Content[0].Text, r2.Value.Content[0].Text);
        }

        public async Task<DtoRolesOutputs> DetermineRoles(DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoRolesInputs.DetermineRolesPrompt();

            var result = await SingleShotChat(prompt, cancellationToken);

            return PromptEngineering.BuildRolesResult(result);
        }

        public async Task<DtoOrganiseOutputs> GroupTasks(DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoOrganiseInputs.GroupTasksPrompt();

            var result = await SingleShotChat(prompt, cancellationToken);

            return dtoOrganiseInputs.GroupTasksResult(result);
        }

        public async Task<DtoUnknownOutputs> IdentifyUnknowns(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            var prompt = dtoBreakdownInputs.DiscoverUnknownsPrompt();

            var result = await SingleShotChat(prompt, cancellationToken);

            return dtoBreakdownInputs.BuildUnknownResult(result);
        }
    }
}
