using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using UtilityDelta.Projects.Interfaces;

namespace UtilityDelta.Projects.Shared
{
    public class AssistantCache(IReadEvents readEvents, IWriteEvents writeEvents): IAssistantCache
    {
        private ConcurrentDictionary<string, string?> _projectToAssistantId = new();

        public string? GetAssistantId(string projectId, CancellationToken cancellationToken)
        {
            if (_projectToAssistantId.TryGetValue(projectId, out var assistantId))
            {
                return assistantId;
            }

            //Search for latest assistantId in project
            var createOrDeleteEvents = readEvents.Read(projectId, 0, cancellationToken, null, null, [ProjectEventType.CreateAssistant, ProjectEventType.DeleteAssistant]);
            if (createOrDeleteEvents.events.Count == 0)
            {
                _projectToAssistantId.AddOrUpdate(projectId, (_) => null, (_, _) => null);
                return null;
            }

            var lastEvent = createOrDeleteEvents.events.Last();
            if (lastEvent.tp == ProjectEventType.DeleteAssistant)
            {
                _projectToAssistantId.AddOrUpdate(projectId, (_) => null, (_, _) => null);
                return null;
            }

            assistantId = lastEvent.t1!;
            _projectToAssistantId.AddOrUpdate(projectId, (_) => assistantId, (_, _) => assistantId);
            return assistantId;
        }

        public ProjectEventItem CreateAssistant(string projectId, string assistantId, string currentUserHash, CancellationToken cancellationToken)
        {
            var eventItem = writeEvents.WriteServerEvent(new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.CreateAssistant, t1: assistantId, null, null, null), projectId);
            _projectToAssistantId.AddOrUpdate(projectId, (_) => assistantId, (_, _) => assistantId);
            return eventItem;
        }

        public ProjectEventItem DeleteAssistant(string projectId, string assistantId, string currentUserHash, CancellationToken cancellationToken)
        {
            var eventItem = writeEvents.WriteServerEvent(new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.DeleteAssistant, t1: assistantId, null, null, null), projectId);
            _projectToAssistantId.AddOrUpdate(projectId, (_) => null, (_, _) => null);
            return eventItem;
        }

        public ProjectEventItem UploadFile(string projectId, string assistantId, string fileId, string fileName, string iv, string currentUserHash, CancellationToken cancellationToken)
        {
            return writeEvents.WriteServerEvent(new ProjectEventItem(0, currentUserHash, 0, iv, ProjectEventType.AddAssistantFile, t1: assistantId, t2: fileId, t3: fileName, null), projectId);
        }

        public ProjectEventItem RemoveFile(string projectId, string assistantId, string fileId, string currentUserHash, CancellationToken cancellationToken)
        {
            return writeEvents.WriteServerEvent(new ProjectEventItem(0, currentUserHash, 0, null, ProjectEventType.DeleteAssistantFile, t1: assistantId, t2: fileId, null, null), projectId);
        }
    }
}
