using Microsoft.AspNetCore.Mvc;
using System.Collections.Concurrent;
using System.Reflection.Metadata;
using System.Xml.Linq;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Services
{
    public class Endpoints(IAssistantManager assistantManager, IAccessLogic accessLogic, IReadEvents readEvents, IWriteAndBackup writeAndBackup, IShareKeyCache shareKeyCache, IUserAccessCache userAccessCache, ISelectLLMProvider llmProcessing) : IEndpoints
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
            return await Task.Run(async () =>
            {
                await accessLogic.PullFromCloudIfNotPresentLocally(pi);

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

        private static ConcurrentDictionary<string, int> _pingCount = new ConcurrentDictionary<string, int>();
        private static ConcurrentDictionary<string, DateTime> _pingLastAccess = new ConcurrentDictionary<string, DateTime>();

        public IResult Ping(
            [FromQuery] string pi)
        {
            _pingCount.AddOrUpdate(pi, 1, (pi, v) => v + 1);
            _pingLastAccess.AddOrUpdate(pi, DateTime.UtcNow, (pi, v) => DateTime.UtcNow);

            return Results.Ok();
        }

        public class DtoPingResult()
        {
            public string pi { get; set; } = string.Empty;
            public int count { get; set; }
            public DateTime lastAccess { get; set; }
        }

        public IResult PingResults(string secret)
        {
            if (secret != "LKJSDFLKJASDFLKJA") return Results.StatusCode(StatusCodes.Status403Forbidden);

            var keys = _pingCount.Keys;
            var result = new List<DtoPingResult>(keys.Count);

            foreach (var key in keys)
            {
                try
                {
                    var count = _pingCount[key];
                    var lastAccess = _pingLastAccess[key];
                    result.Add(new DtoPingResult { pi = key, count = count, lastAccess = lastAccess });
                }
                catch
                {
                }
            }

            return Results.Ok(result);
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
                    ProjectAccess.OwnerAccess => Results.Ok(new DtoDisableAccess(userAccessCache.UpdateAccess(pi, accessInfo.CurrentUserHash, userId, null, null, null, true, null, null, cancellationToken))),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> DeleteAllFiles(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            CancellationToken cancellationToken)
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
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.OwnerAccess or ProjectAccess.WriteAccess => Results.Ok(await assistantManager.DeleteAllFiles(pi, accessInfo.CurrentUserHash)),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> DeleteFile(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] string fileId,
            CancellationToken cancellationToken)
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
                    ProjectAccess.NotExists => Results.NotFound(),
                    ProjectAccess.OwnerAccess or ProjectAccess.WriteAccess => Results.Ok(await assistantManager.DeleteFile(pi, accessInfo.CurrentUserHash, fileId)),
                    _ => Results.StatusCode(StatusCodes.Status403Forbidden)
                };
            });
        }

        public async Task<IResult> UploadFile(
            [FromQuery] string pi,
            [FromQuery] string publicKey,
            [FromQuery] string nonce,
            [FromQuery] string sign,
            [FromQuery] string system,
            [FromQuery] string iv,
            [FromQuery] string encrypted_fileName,
            IFormFile document,
            CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: true,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                if (accessInfo.ProjectAccess == ProjectAccess.NotExists) return Results.NotFound();
                if (accessInfo.ProjectAccess == ProjectAccess.NoAccess || accessInfo.ProjectAccess == ProjectAccess.ReadOnlyAccess) return Results.StatusCode(StatusCodes.Status403Forbidden);

                using var documentStream = document.OpenReadStream();
                return Results.Ok((await assistantManager.UploadFile(pi, accessInfo.CurrentUserHash, system, encrypted_fileName, Path.GetExtension(document.FileName), iv, documentStream, cancellationToken)));
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
                    createProjectIfNotExists: true,
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
                var creatorEventDate = events.Min(x => x.ed) - 1;

                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: createIfNotExist,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    edOverride: creatorEventDate,
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.BreakdownQuestions(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.BreakdownQuestions(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.BreakdownTask(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.BreakdownTask(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken))
                };
            });
        }

        public async Task<IResult> ImageBreakdown(
            [FromQuery] string pi, 
            [FromQuery] string publicKey, 
            [FromQuery] string nonce, 
            [FromQuery] string sign, 
            [FromQuery] string system, 
            [FromQuery] string task,
            [FromQuery] string fileName,
            IFormFile image,
            CancellationToken cancellationToken)
        {
            return await Task.Run(async () =>
            {
                var dtoImageBreakdownInputs = new DtoImageBreakdownInputs()
                {
                    system = system,
                    task = task
                };
                using var documentStream = image.OpenReadStream();

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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.ImageBreakdownTask(pi, accessInfo.CurrentUserHash, fileName, documentStream, dtoImageBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.ImageBreakdownTask(pi, accessInfo.CurrentUserHash, fileName, documentStream, dtoImageBreakdownInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.IdentifyUnknowns(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.IdentifyUnknowns(pi, accessInfo.CurrentUserHash, dtoBreakdownInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.DetermineRoles(pi, accessInfo.CurrentUserHash, dtoRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.DetermineRoles(pi, accessInfo.CurrentUserHash, dtoRolesInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.AssignRoles(pi, accessInfo.CurrentUserHash, dtoAssignRolesInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.AssignRoles(pi, accessInfo.CurrentUserHash, dtoAssignRolesInputs, cancellationToken))
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
                    ProjectAccess.NotExists => Results.Ok(await llmProcessing.GroupTasks(pi, accessInfo.CurrentUserHash, dtoOrganiseInputs, cancellationToken)),
                    ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                    _ => Results.Ok(await llmProcessing.GroupTasks(pi, accessInfo.CurrentUserHash, dtoOrganiseInputs, cancellationToken))
                };
            });
        }
    }
}
