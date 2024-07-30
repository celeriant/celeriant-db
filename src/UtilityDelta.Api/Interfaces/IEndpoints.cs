using Microsoft.AspNetCore.Mvc;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IEndpoints
    {
        Task<IResult> DisableShare([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] string shareKeyHash, CancellationToken cancellationToken);
        Task<IResult> DisableUser([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] string userId, CancellationToken cancellationToken);
        Task<IResult> Read([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] long fromTime, [FromQuery] bool createIfNotExist, [FromQuery] string? shareKey, CancellationToken cancellationToken);
        Task<IResult> Share([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] bool isOwner, [FromQuery] bool singleUse, [FromQuery] string? iv, [FromQuery] string? description, [FromQuery] long expiresOn, [FromQuery] bool readOnly, CancellationToken cancellationToken);
        Task<IResult> Write([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] bool createIfNotExist, [FromBody] ProjectEventItem[] events, CancellationToken cancellationToken);
        Task<IResult> Breakdown([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Unknowns([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Roles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<IResult> AssignRoles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoAssignRolesInputs dtoAssignRolesInputs, CancellationToken cancellationToken);
    }
}
