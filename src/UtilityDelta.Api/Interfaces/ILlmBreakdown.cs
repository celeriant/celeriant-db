using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface ILlmBreakdown
    {
        Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, string currentUserHash, string pi, CancellationToken cancellationToken);
        Task<DtoUnknownOutputs> IdentifyUnknowns(DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<DtoRolesOutputs> DetermineRoles(DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoAssignRolesOutputs> AssignRoles(DtoAssignRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<DtoOrganiseOutputs> GroupTasks(DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken);
    }
}
