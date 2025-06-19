using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Xml.Linq;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Services
{
    public class FileHandlesManager(IOptions<SystemSettings> utilityDeltaConfiguration) : IFileHandlesManager
    {
        //Does not need to be concurrent as we are only using this inside the GetOrAdd lambda
        private readonly Queue<string> _containerQueue = new();

        private readonly ConcurrentDictionary<string, FileStream> _containerFileStreams = new();
        private readonly ConcurrentDictionary<string, int> _isInUse = new();

        public int NumberOfOpenStreams => _containerFileStreams.Count;

        public int NumberOfConnectionsToContainer(string container)
        {
            if (_isInUse.TryGetValue(container, out var streamCount)) return streamCount;
            return 0;
        }

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
                //Queue allows us to determine which stream to dispose when we start getting to out open file limit
                _containerQueue.Enqueue(container);

                //Open write handle but allow other threads to still read (don't exclusively lock the file)
                //We must open as read/write so that we can read the last used incremented ID
                Directory.CreateDirectory(utilityDeltaConfiguration.Value.SUB_DIR_CONTAINERS);
                var filePath = container.ContainerPath(utilityDeltaConfiguration.Value.SUB_DIR_CONTAINERS);
                return new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            });

            //Evict the stream that is next in the queue
            HashSet<string> bailContainers = new();
            while (ReachedOpenLimit && _containerQueue.TryDequeue(out var deqContainer) && _containerFileStreams.TryGetValue(deqContainer, out var deqFileStream))
            {
                //Prevent looping forever
                if (bailContainers.Contains(deqContainer))
                {
                    _containerQueue.Enqueue(deqContainer);
                    break;
                }

                if (DisposeIfStreamNotInUse(deqContainer, deqFileStream))
                {
                    _containerFileStreams.TryRemove(deqContainer, out var _);
                }
                else
                {
                    //Put it back in the queue as still in use
                    _containerQueue.Enqueue(deqContainer);
                    bailContainers.Add(deqContainer);
                }
            }

            return new FileHandles(stream, container, _containerFileStreams, _isInUse, DisposeIfStreamNotInUse);
        }

        private bool ReachedOpenLimit => _containerFileStreams.Keys.Count > utilityDeltaConfiguration.Value.FILE_HANDLE_OPEN_LIMIT;

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
        private bool DisposeIfStreamNotInUse(string contaner, FileStream? stream)
        {
            if (!StreamNotInUse(contaner))
            {
                return false;
            }

            //Don't really care about this if throws
            try { stream?.Dispose(); } catch { }

            //Already <= 0, but try to free memory by removing it entirely
            _isInUse.TryRemove(contaner, out var _);

            return true;
        }

        public void Delete(string container)
        {
            var hasValue = _containerFileStreams.TryGetValue(container, out var stream);
            if (hasValue && stream != null)
            {
                stream.Dispose();
            }

            var filePath = container.ContainerPath(utilityDeltaConfiguration.Value.SUB_DIR_CONTAINERS);
            File.Delete(filePath);

            _isInUse.TryRemove(container, out var _);
            _containerFileStreams.TryRemove(container, out var _);
        }
    }
}
