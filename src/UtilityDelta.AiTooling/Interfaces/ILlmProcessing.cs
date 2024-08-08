using UtilityDelta.AiTooling.Dtos;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface ILlmProcessing
    {
        Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoUnknownOutputs> IdentifyUnknowns(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoRolesOutputs> DetermineRoles(DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoAssignRolesOutputs> AssignRoles(DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoOrganiseOutputs> GroupTasks(DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken);
    }
}
