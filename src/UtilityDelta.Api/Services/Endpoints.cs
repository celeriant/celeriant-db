using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class Endpoints(IAccessLogic accessLogic, IReadEvents readEvents, IWriteAndBackup writeAndBackup, IShareKeyCache shareKeyCache, IUserAccessCache userAccessCache, ILlmBreakdown llmBreakdown) : IEndpoints
    {
        public async Task<IResult> Read(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] long fromTime,
            [FromQuery] bool createIfNotExist,
            [FromQuery] string? shareKey,
            CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: createIfNotExist && fromTime == 0,
                    shareKey: shareKey,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(readEvents.Read(pi, fromTime, cancellationToken, accessInfo.CurrentUserHash))
                };
            });
        }

        public async Task<IResult> DisableShare(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] string shareKeyHash,
            CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
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
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.OwnerAccess => Results.Ok(new DtoDisableAccess(shareKeyCache.MarkShareKeyAsUsed(pi, accessInfo.CurrentUserHash, shareKeyHash, cancellationToken))),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> DisableUser(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] string userId,
            CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
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
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.OwnerAccess => Results.Ok(new DtoDisableAccess(userAccessCache.UpdateAccess(pi, accessInfo.CurrentUserHash, userId, null, null, null, true, null, cancellationToken))),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> Share(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] bool isOwner,
            [FromQuery] bool singleUse,
            [FromQuery] string? iv,
            [FromQuery] string? description,
            [FromQuery] long expiresOn,
            [FromQuery] bool readOnly,
            CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
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
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.OwnerAccess => Results.Ok(shareKeyCache.CreateShareLink(pi, accessInfo.CurrentUserHash, isOwner, singleUse, iv, description, expiresOn, readOnly, cancellationToken)),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> Write(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] bool createIfNotExist,
            [FromBody] ProjectEventItem[] events,
            CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: createIfNotExist,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                return accessInfo.ProjectAccess switch
                {
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(writeAndBackup.WriteClientEvents(events, accessInfo.CurrentUserHash, pi, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmBreakdown.BreakdownTask(dtoBreakdownInputs, accessInfo.CurrentUserHash, pi, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmBreakdown.BreakdownTask(dtoBreakdownInputs, accessInfo.CurrentUserHash, pi, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmBreakdown.IdentifyUnknowns(dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmBreakdown.IdentifyUnknowns(dtoBreakdownInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmBreakdown.DetermineRoles(dtoRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmBreakdown.DetermineRoles(dtoRolesInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmBreakdown.AssignRoles(dtoAssignRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmBreakdown.AssignRoles(dtoAssignRolesInputs, cancellationToken))
                };
            });
        }
    }
}
