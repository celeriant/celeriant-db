using OpenAI_API.Models;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ChatGPTBreakdown : ILlmBreakdown
    {
        public async Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, string currentUserHash, string pi, CancellationToken cancellationToken)
        {
            var prompt = LlmBreakdown.InitialPrompt(dtoBreakdownInputs);

            var api = new OpenAI_API.OpenAIAPI("OPENAI_KEY_REDACTED_PRE_PUBLICATION_SEE_PROVENANCE_MD");
            
            var chat = api.Chat.CreateConversation();
            if (!string.IsNullOrEmpty(dtoBreakdownInputs.system))
            {
                chat.AppendSystemMessage(dtoBreakdownInputs.system);
            }
            chat.Model = Model.GPT4_Turbo;
            chat.AppendUserInput(prompt);

            string r1 = await chat.GetResponseFromChatbotAsync();

            chat.AppendUserInput($"List any dependencies between the tasks, one line at a time, in a similar format, using '->'");

            string r2 = await chat.GetResponseFromChatbotAsync();

            return LlmBreakdown.BuildResult(dtoBreakdownInputs, r1, r2);
        }
    }
}
