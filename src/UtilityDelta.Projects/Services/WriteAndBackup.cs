using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Services
{
    public class WriteAndBackup(ILogger<WriteAndBackup> logger, IFileHandlesManager fileHandlesManager, IWriteEvents writeEvents, IOptions<SystemSettings> utilityDeltaConfiguration) : IWriteAndBackup
    {
        private readonly ConcurrentQueue<string> _projectsForBackup = new ConcurrentQueue<string>();
        private readonly ConcurrentDictionary<string, DateTime> _projectsInQueue = new ConcurrentDictionary<string, DateTime>();

        public DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken)
        {
            var result = writeEvents.WriteClientEvents(events, createdBy, pi, cancellationToken);

            TryQueueBackupForProject(pi);

            return result;
        }

        private void TryQueueBackupForProject(string pi)
        {
            if (!_projectsInQueue.ContainsKey(pi))
            {
                _projectsInQueue.TryAdd(pi, DateTime.UtcNow);
                _projectsForBackup.Enqueue(pi);
            }
        }

        public async Task ProcessQueue()
        {
            while (true)
            {
                await Task.Delay(utilityDeltaConfiguration.Value.CLOUD_UPLOAD_FREQUENCY);

                if (_projectsForBackup.Count > 100)
                {
                    //For now just log a warning. Later we should do a 'burst' where we
                    //upload all projects in the queue that have been waiting longer than a set period
                    logger.LogWarning("Backup queue length too long: {len}", _projectsForBackup.Count);
                }

                if (_projectsForBackup.TryDequeue(out var pi))
                {
                    _projectsInQueue.TryRemove(pi, out var queuedTime);

                    try
                    {
                        await WriteToCloud(pi);
                    }
                    catch (Exception ex)
                    {
                        logger.LogCritical(ex, "Unable to upload project to cloud: {pi} due to {message}", pi, ex.Message);
                    }
                }
            }
        }

        private async Task WriteToCloud(string pi)
        {
            logger.LogInformation("Writing project to cloud storage: {pi}", pi);

            await Task.Run(() =>
            {
                var blobServiceClient = new BlobServiceClient(utilityDeltaConfiguration.Value.CLOUD_STORAGE_CONNECTION);
                var blobContainerClient = blobServiceClient.GetBlobContainerClient(Environment.MachineName.Replace("-", "").ToLowerInvariant());
                blobContainerClient.CreateIfNotExists();

                var appendBlobClient = blobContainerClient.GetAppendBlobClient(pi);

                // Ensure the Append Blob exists
                if (!appendBlobClient.Exists() || appendBlobClient.GetProperties().Value.BlobCommittedBlockCount > utilityDeltaConfiguration.Value.APPEND_BLOB_MAX_COMMITS)
                {
                    if (appendBlobClient.Exists())
                    {
                        appendBlobClient.Delete();
                    }
                    appendBlobClient.Create();
                }

                // Get the size of the existing blob
                long existingBlobSize = appendBlobClient.GetProperties().Value.ContentLength;

                //This call to get the stream is thread safe
                using var fileHandle = fileHandlesManager.OpenWrite(pi);

                //Must lock while ripping out data from disk into memory so others can't edit the file
                var streamsToAppend = new List<MemoryStream>();
                lock (fileHandle.Stream)
                {
                    streamsToAppend = GetFileChunks(fileHandle.Stream, existingBlobSize);
                }

                foreach (var chunk in streamsToAppend)
                {
                    chunk.Seek(0, SeekOrigin.Begin);
                    appendBlobClient.AppendBlock(chunk);
                }
            });

            logger.LogInformation("Completed uploading project to cloud storage: {pi}", pi);
        }

        private List<MemoryStream> GetFileChunks(FileStream fileStream, long startPosition)
        {
            var memoryStreams = new List<MemoryStream>();

            // Ensure the starting position is within the file length
            if (startPosition < 0 || startPosition >= fileStream.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(startPosition), "Start position is out of the range of the file stream length.");
            }

            // Set the stream position to the starting position
            fileStream.Seek(startPosition, SeekOrigin.Begin);

            byte[] buffer = new byte[utilityDeltaConfiguration.Value.APPEND_BLOB_MAX_CHUNK_SIZE];
            int bytesRead;
            while ((bytesRead = fileStream.Read(buffer, 0, utilityDeltaConfiguration.Value.APPEND_BLOB_MAX_CHUNK_SIZE)) > 0)
            {
                var memoryStream = new MemoryStream(buffer, 0, bytesRead);
                memoryStreams.Add(memoryStream);
            }

            return memoryStreams;
        }

        public ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi)
        {
            var writeResult = writeEvents.WriteServerEvent(eventItem, pi);

            TryQueueBackupForProject(pi);

            return writeResult;
        }
    }
}
