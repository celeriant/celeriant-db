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
        private static ConcurrentDictionary<string, string> UserToProject { get; set; } = new ConcurrentDictionary<string, string>();

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            UserToProject.TryRemove(Context.ConnectionId, out _);
            return base.OnDisconnectedAsync(exception);
        }

        public async Task JoinProject(string pi, string publicKey, string nonce, string sign)
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                projectId: pi,
                createProjectIfNotExists: false,
                shareKey: null,
                publicKey: publicKey,
                nonce: nonce,
                sign: sign,
                cancellationToken: CancellationToken.None);

            if (accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NoAccess || accessInfo.ProjectAccess == Projects.Shared.ProjectAccess.NotExists) return;

            UserToProject.TryRemove(Context.ConnectionId, out _);
            UserToProject.TryAdd(Context.ConnectionId, pi);
            await Groups.AddToGroupAsync(Context.ConnectionId, pi);
        }

        public async Task LeaveProject()
        {
            UserToProject.TryRemove(Context.ConnectionId, out var pi);
            if (pi != null)
            {
                await Groups.RemoveFromGroupAsync(Context.ConnectionId, pi);
            }
        }

        public async Task AddedEvents()
        {
            if (UserToProject.TryGetValue(Context.ConnectionId, out var pi))
            {
                await Clients.OthersInGroup(pi).SendAsync("NewEvents");
            }
        }
    }
}
