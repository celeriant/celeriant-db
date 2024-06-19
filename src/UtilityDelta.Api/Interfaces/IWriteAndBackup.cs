using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteAndBackup
    {
        Task ProcessQueue();
        DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken);
        ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi);
    }
}
