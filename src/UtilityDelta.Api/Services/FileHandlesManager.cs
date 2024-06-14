using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class FileHandlesManager(IOptions<ConfigurationEntry> utilityDeltaConfiguration)
: IFileHandlesManager
    {
        //Does not need to be concurrent as we are only using this inside the GetOrAdd lambda
        private readonly Queue<string> _containerQueue = new();

        private readonly ConcurrentDictionary<string, FileStream> _containerFileStreams = new();
        private readonly ConcurrentDictionary<string, int> _isInUse = new();

        public bool Exists(string container)
        {
            var path = container.ContainerPath(utilityDeltaConfiguration.Value.SUB_DIR_CONTAINERS);
            return File.Exists(path);
        }

        public FileHandles OpenWrite(string container)
        {
            //Keep track of how many threads are using the FileStream for this container
            _isInUse.AddOrUpdate(container, 1, (_, existingValue) => existingValue + 1);

            //create a FileStream if none exists yet, or use the existing FileStream if already one present without creating duplicate FileStreams
            var stream = _containerFileStreams.GetOrAdd(container, _ =>
            {
                //Mark a container as 'evicted' from the active list by removing it from _containerFileStreams
                if (ReachedOpenLimit && _containerQueue.TryDequeue(out var deqContainer))
                {
                    _containerFileStreams.Remove(deqContainer, out var deqFileStream);
                    DisposeIfStreamNotInUse(deqContainer, deqFileStream);
                }

                //Queue allows us to determine which stream to dispose when we start getting to out open file limit
                _containerQueue.Enqueue(container);

                //Open write handle but allow other threads to still read (don't exclusively lock the file)
                //We must open as read/write so that we can read the last used incremented ID
                return new FileStream(container.ContainerPath(utilityDeltaConfiguration.Value.SUB_DIR_CONTAINERS), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            });

            return new FileHandles(stream, container, _containerFileStreams, _isInUse, DisposeIfStreamNotInUse);
        }

        private bool ReachedOpenLimit => _containerFileStreams.Keys.Count + 1 > utilityDeltaConfiguration.Value.FILE_HANDLE_OPEN_LIMIT;

        /// <summary>
        /// Checks the static global '_isInUse' to see if any threads are using the FileStream for this container
        /// </summary>
        private bool StreamNotInUse(string container)
        {
            var hasValue = _isInUse.TryGetValue(container, out var deqContainer);

            return !hasValue || deqContainer <= 0;
        }

        /// <summary>
        /// Dispose of the FileStream, releasing the OS file handle. We can only do this
        /// if the FileStream is not used by any other thread.
        /// </summary>
        private void DisposeIfStreamNotInUse(string contaner, FileStream? stream)
        {
            if (!StreamNotInUse(contaner))
            {
                return;
            }

            //Don't really care about this if throws
            try { stream?.Dispose(); } catch { }

            //Already <= 0, but try to free memory by removing it entirely
            _isInUse.TryRemove(contaner, out var _);
        }
    }
}
