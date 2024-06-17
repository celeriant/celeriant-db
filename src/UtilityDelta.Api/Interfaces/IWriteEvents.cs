using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteEvents
    {
        DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken);

        ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi);

        DtoWrite CustomWriteEvents(ProjectEventItem[] events, string pi, CancellationToken cancellationToken);
    }
}
