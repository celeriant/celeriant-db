using Microsoft.AspNetCore.Mvc;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Services
{
    public class Endpoints(ILlmProcessing llmProcessing, IAccessLogic accessLogic) : IEndpoints
    {
        public async Task<IResult> BreakdownQuestions([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.BreakdownQuestions(dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.BreakdownQuestions(dtoBreakdownInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> Breakdown([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.BreakdownTask(dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.BreakdownTask(dtoBreakdownInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> Unknowns([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoBreakdownInputs dtoBreakdownInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.IdentifyUnknowns(dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.IdentifyUnknowns(dtoBreakdownInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> Roles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoRolesInputs dtoRolesInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.DetermineRoles(dtoRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.DetermineRoles(dtoRolesInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> AssignRoles([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoAssignRolesInputs dtoAssignRolesInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.AssignRoles(dtoAssignRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.AssignRoles(dtoAssignRolesInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> GroupTasks([FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, [FromBody] DtoOrganiseInputs dtoOrganiseInputs, CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.GroupTasks(dtoOrganiseInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.GroupTasks(dtoOrganiseInputs, cancellationToken))
                };
            });
        }
    }
}
