using Microsoft.AspNetCore.Mvc;
using UtilityDelta.AiTooling.Dtos;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IEndpoints
    {
        Task<IResult> BreakdownQuestions([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Breakdown([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Unknowns([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Roles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<IResult> AssignRoles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoAssignRolesInputs dtoAssignRolesInputs, CancellationToken cancellationToken);
        Task<IResult> GroupTasks([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken);
    }
}
