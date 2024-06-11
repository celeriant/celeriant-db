using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using UtilityDelta.WebAPI.RealTime;

namespace UtilityDelta.Realtime
{
    public class UtilityDeltaHub : Hub
    {
        private static ConcurrentDictionary<string, string> UserToProject { get; set; } = new ConcurrentDictionary<string, string>();

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            UserToProject.TryRemove(Context.ConnectionId, out _);
            return base.OnDisconnectedAsync(exception);
        }

        public async Task JoinProject(string pi)
        {
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
