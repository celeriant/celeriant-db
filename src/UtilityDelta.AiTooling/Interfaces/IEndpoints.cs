using Microsoft.AspNetCore.Mvc;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Interfaces
{
    public interface IEndpoints
    {
        Task<IResult> UploadFile([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] string system, [FromQuery] string iv, [FromQuery]string encrypted_fileName, IFormFile document, CancellationToken cancellationToken);
        Task<IResult> DeleteFile(string pi, string publicKey, string nonce, string sign, string fileId, CancellationToken cancellationToken);
        Task<IResult> DeleteAllFiles(string pi, string publicKey, string nonce, string sign, CancellationToken cancellationToken);
        Task<IResult> DisableShare([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] string shareKeyHash, CancellationToken cancellationToken);
        Task<IResult> DisableUser([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] string userId, CancellationToken cancellationToken);
        Task<IResult> Read([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] long fromTime, [FromQuery] bool createIfNotExist, [FromQuery] string? shareKey, CancellationToken cancellationToken);
        IResult Ping([FromQuery] string pi);
        IResult PingResults([FromQuery] string key);
        Task<IResult> Share([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] bool isOwner, [FromQuery] bool singleUse, [FromQuery] string? iv, [FromQuery] string? description, [FromQuery] long expiresOn, [FromQuery] bool readOnly, CancellationToken cancellationToken);
        Task<IResult> Write([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromQuery] bool createIfNotExist, [FromBody] ProjectEventItem[] events, CancellationToken cancellationToken);
        Task<IResult> BreakdownQuestions([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Breakdown([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Unknowns([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken);
        Task<IResult> Roles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken);
        Task<IResult> AssignRoles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoAssignRolesInputs dtoAssignRolesInputs, CancellationToken cancellationToken);
        Task<IResult> GroupTasks([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken);
    }
}
