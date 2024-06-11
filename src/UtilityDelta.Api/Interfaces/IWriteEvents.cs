using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteEvents
    {
        (long lastServerId, long eventDate) Write(ProjectEventItem[] events, string createdBy, string pi);
    }
}
