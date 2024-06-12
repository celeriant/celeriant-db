using System;
using System.Linq;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Interfaces
{
    public interface IReadEvents
    {
        bool Exists(string container);
        DtoRead Read(string container, long fromEventId, string currentUser);
    }
}
