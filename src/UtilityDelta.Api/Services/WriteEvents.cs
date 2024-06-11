using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class WriteEvents : IWriteEvents
    {
        private const uint EVENT_VERSION = 1;

        public (long lastServerId, long eventDate) Write(ProjectEventItem[] events, string createdBy, string pi)
        {
            //This call to get the stream is thread safe
            using var fileHandle = FileHandles.OpenWrite(pi);

            //Must lock while writing to disk - only one writer at a time.
            lock (fileHandle.Stream)
            {
                var latestId = GetLatestId(fileHandle);
                fileHandle.Stream.Seek(0, SeekOrigin.End);

                using var binaryWriter = new BinaryWriter(fileHandle.Stream, Encoding.UTF8, true);

                var eventDate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                foreach (var item in events)
                {
                    latestId++;
                    WriteEvent(binaryWriter, createdBy, item.iv, (ushort)item.tp, eventDate, latestId, item.n1, item.t1, item.t2, item.t3);
                }

                binaryWriter.Flush();

                return (latestId, eventDate);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteEvent(BinaryWriter binaryWriter, string cb, string? iv, ushort et, long ed, long id, double? n1, string? t1, string? t2, string? t3)
        {
            var pos1 = binaryWriter.BaseStream.Position;

            binaryWriter.Write(EVENT_VERSION);
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
