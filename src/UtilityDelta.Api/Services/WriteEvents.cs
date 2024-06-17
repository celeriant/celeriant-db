using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using UtilityDelta.Api.Exceptions;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class WriteEvents(IFileHandlesManager fileHandlesManager) : IWriteEvents
    {
        public DtoWrite WriteClientEvents(ProjectEventItem[] events, string createdBy, string pi, CancellationToken cancellationToken)
        {
            return InternalWrite(events.Where(x => !x.tp.IsServerEvent()), createdBy, pi, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), cancellationToken);
        }

        public DtoWrite CustomWriteEvents(ProjectEventItem[] events, string pi, CancellationToken cancellationToken)
        {
            return InternalWrite(events, null, pi, null, cancellationToken);
        }

        private DtoWrite InternalWrite(IEnumerable<ProjectEventItem> events, string? createdBy, string pi, long? eventDate, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) throw new ExceptionCancelledOperation();

            //This call to get the stream is thread safe
            using var fileHandle = fileHandlesManager.OpenWrite(pi);

            //Must lock while writing to disk - only one writer at a time.
            lock (fileHandle.Stream)
            {
                var latestId = GetLatestId(fileHandle);
                fileHandle.Stream.Seek(0, SeekOrigin.End);

                using var binaryWriter = new BinaryWriter(fileHandle.Stream, Encoding.UTF8, true);

                foreach (var item in events)
                {
                    if (cancellationToken.IsCancellationRequested) throw new ExceptionCancelledOperation();

                    latestId++;
                    WriteEvent(binaryWriter, createdBy ?? item.cb, item.iv, (ushort)item.tp, eventDate ?? item.ed, latestId, item.n1, item.t1, item.t2, item.t3);
                }

                binaryWriter.Flush();

                return new DtoWrite(latestId, eventDate ?? 0);
            }
        }

        public ProjectEventItem WriteServerEvent(ProjectEventItem eventItem, string pi)
        {
            var writeResult = InternalWrite([eventItem], eventItem.cb, pi, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), CancellationToken.None);
            return new ProjectEventItem(writeResult.serverId, eventItem.cb, writeResult.eventDate, eventItem.iv, eventItem.tp, eventItem.t1, eventItem.t2, eventItem.t3, eventItem.n1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteEvent(BinaryWriter binaryWriter, string? cb, string? iv, ushort et, long ed, long id, double? n1, string? t1, string? t2, string? t3)
        {
            var pos1 = binaryWriter.BaseStream.Position;

            binaryWriter.Write(Constants.EVENT_VERSION);
            binaryWriter.WriteNullable(t1);
            binaryWriter.WriteNullable(t2);
            binaryWriter.WriteNullable(t3);
            binaryWriter.WriteNullable(n1);
            binaryWriter.WriteNullable(iv);
            binaryWriter.Write(et);
            binaryWriter.Write(ed);
            binaryWriter.WriteNullable(cb);
            binaryWriter.Write(id);
            binaryWriter.Write((int)(binaryWriter.BaseStream.Position - pos1));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long GetLatestId(FileHandles fileHandle)
        {
            if (fileHandle.Stream.Length == 0) return 0;

            fileHandle.Stream.Seek(-1 * Constants.OFFSET_BYTES_FOR_GETTING_EVENTID, SeekOrigin.End);

            using var binaryReader = new BinaryReader(fileHandle.Stream, Encoding.UTF8, true);
            return binaryReader.ReadInt64();
        }
    }
}
