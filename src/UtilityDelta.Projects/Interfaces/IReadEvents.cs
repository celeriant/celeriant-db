using System;
using System.Linq;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IReadEvents
    {
        DtoRead Read(string container, long fromEventId, CancellationToken cancellationToken, string? currentUser = null, ProjectEventType? filterEventType = null, HashSet<ProjectEventType>? multiFilterEventType = null);
    }
}
