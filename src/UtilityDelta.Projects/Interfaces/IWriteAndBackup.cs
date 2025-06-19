using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IWriteAndBackup
    {
        Task<bool> ReadFromCloud(string pi);
        Task ProcessQueue();
        DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken);
        ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi);
        bool DeleteProject(string pi, string currentUserHash);
    }
}
