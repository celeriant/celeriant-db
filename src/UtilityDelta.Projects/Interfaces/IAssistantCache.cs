using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Interfaces
{
    public interface IAssistantCache
    {
        ProjectEventItem RemoveFile(string projectId, string assistantId, string fileId, string currentUserHash, CancellationToken cancellationToken);
        ProjectEventItem UploadFile(string projectId, string assistantId, string fileId, string fileName, string iv, string currentUserHash, CancellationToken cancellationToken);
        ProjectEventItem CreateAssistant(string projectId, string assistantId, string currentUserHash, CancellationToken cancellationToken);
        ProjectEventItem DeleteAssistant(string projectId, string assistantId, string currentUserHash, CancellationToken cancellationToken);
        string? GetAssistantId(string projectId, CancellationToken cancellationToken);
    }
}
