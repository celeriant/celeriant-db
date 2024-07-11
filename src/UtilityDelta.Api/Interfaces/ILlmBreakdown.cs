using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface ILlmBreakdown
    {
        Task<DtoBreakdownOutputs> BreakdownTask(DtoBreakdownInputs dtoBreakdownInputs, string currentUserHash, string pi, CancellationToken cancellationToken);
    }
}
