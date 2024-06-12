using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IWriteEvents
    {
        DtoWrite Write(ProjectEventItem[] events, string createdBy, string pi);
    }
}
