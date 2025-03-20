using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.WebAPI.RealTime;

namespace UtilityDelta.Realtime
{
    public class UtilityDeltaHub(IAccessLogic accessLogic) : Hub
    {
        public async Task JoinProject(string projectId, string publicKey, string nonce, string sign)
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                projectId: projectId,
                createProjectIfNotExists: false,
                shareKey: null,
                publicKey: publicKey,
                nonce: nonce,
                sign: sign,
                cancellationToken: CancellationToken.None);

            if (accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NoAccess || accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NotExists) return;

            await Groups.AddToGroupAsync(Context.ConnectionId, projectId);
        }

        public async Task LeaveProject(string projectId)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, projectId);
        }

        public async Task AddedEvents(string projectId, string publicKey, string nonce, string sign)
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                projectId: projectId,
                createProjectIfNotExists: false,
                shareKey: null,
                publicKey: publicKey,
                nonce: nonce,
                sign: sign,
                cancellationToken: CancellationToken.None);

            if (accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NoAccess || accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NotExists) return;

            await Clients.OthersInGroup(projectId).SendAsync("NewEvents");
        }
    }
}
