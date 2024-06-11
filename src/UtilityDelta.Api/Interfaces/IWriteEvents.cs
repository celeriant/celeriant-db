using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteEvents
    {
        long Write(ProjectEventItem[] events, string createdBy, string pi);
    }
}
