using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Api.Services
{
    public class Endpoints(IAccessLogic accessLogic, IReadEvents readEvents, IWriteAndBackup writeAndBackup, IShareKeyCache shareKeyCache, IUserAccessCache userAccessCache) : IEndpoints
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
            public string pi { get; set; }
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

    }
}
