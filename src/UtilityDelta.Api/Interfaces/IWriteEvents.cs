using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteEvents
    {
        DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi);

        ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi);
    }
}
