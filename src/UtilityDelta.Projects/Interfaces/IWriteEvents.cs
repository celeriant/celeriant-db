using System;
using System.Linq;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IWriteEvents
    {
        DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken);

        ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi);

        DtoWrite CustomWriteEvents(ProjectEventItem[] events, string pi, CancellationToken cancellationToken);
    }
}
