using System.Collections.Concurrent;

namespace UtilityDelta.Projects.Shared
{
    public sealed class FileHandles(
        FileStream stream, 
        string container, 
        ConcurrentDictionary<string, FileStream> _containerFileStreams, 
        ConcurrentDictionary<string, int> _isInUse, 
        Func<string, FileStream?, bool> _disposeIfStreamNotInUse) : IDisposable
    {
        public FileStream Stream => stream;

        public void Dispose()
        {
            //Here we DO NOT dispose of the stream, instead keeping it in memory ready for the next write operation
            _isInUse.AddOrUpdate(container, 0, (_, existingValue) => existingValue - 1);

            //The exception to this is if this stream has been popped from the queue as we have exceeded the open file count
            //In this case we want to dispose of this orphaned stream
            if (!_containerFileStreams.ContainsKey(container))
            {
                _disposeIfStreamNotInUse(container, Stream);
            }
        }
    }
}
