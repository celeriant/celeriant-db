using UtilityDelta.AiTooling.Dtos;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IAssistantLlmProcessing
    {
        Task<DtoBreakdownOutputs> BreakdownTask(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoBreakdownQuestionsOutputs> BreakdownQuestions(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoUnknownOutputs> IdentifyUnknowns(string projectId, string currentUserHash, DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoRolesOutputs> DetermineRoles(string projectId, string currentUserHash, DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoAssignRolesOutputs> AssignRoles(string projectId, string currentUserHash, DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoOrganiseOutputs> GroupTasks(string projectId, string currentUserHash, DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken);
    }
}
