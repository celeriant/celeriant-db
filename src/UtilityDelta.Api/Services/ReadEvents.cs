using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ReadEvents : IReadEvents
    {

        public List<ProjectEventItem> Read(string container, long fromEventId, string currentUser)
        {
            var path = container.ContainerPath();
            if (!File.Exists(path)) return new List<ProjectEventItem>();

            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new BinaryReader(stream, Encoding.UTF8, true);

            //Step 1 is to find the start of the events we want to read
            if (fromEventId > 0)
            {
                long offsetFromEnd = 0;
                while (true)
                {
                    var eventIdOffset = offsetFromEnd + Constants.OFFSET_BYTES_FOR_GETTING_EVENTID;
                    if (eventIdOffset >= reader.BaseStream.Length) break;

                    stream.Seek(eventIdOffset * -1, SeekOrigin.End);

                    var eventId = reader.ReadInt64();
                    if (eventId <= fromEventId) break;

                    var dataLengthForEvent = reader.ReadInt32();
                    offsetFromEnd = dataLengthForEvent + Constants.SIZEOF_EVENT_SIZE + offsetFromEnd;
                }

                stream.Seek(offsetFromEnd * -1, SeekOrigin.End);
            }

            var events = new List<ProjectEventItem>();
            while (stream.Position < stream.Length)
            {
                var eventItem = ReadEvent(reader);
                if (eventItem.cb == currentUser) continue;

                events.Add(eventItem);
            }

            return events;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ProjectEventItem ReadEvent(BinaryReader binaryReader)
        {
            binaryReader.ReadUInt32(); //Version

            var t1 = binaryReader.ReadStringNullable();
            var t2 = binaryReader.ReadStringNullable();
            var t3 = binaryReader.ReadStringNullable();
            var n1 = binaryReader.ReadDoubleNullable();
            var iv = binaryReader.ReadStringNullable();
            var tp = (ProjectEventType)binaryReader.ReadUInt16();
            var ed = binaryReader.ReadInt64();
            var cb = binaryReader.ReadStringNullable();
            var serverId = binaryReader.ReadInt64();
            binaryReader.ReadInt32(); //totalSize

            return new ProjectEventItem(serverId, cb, ed, iv, tp, t1, t2, t3, n1);
        }
    }
}
