using System;
using System.Linq;
using System.Text;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Services
{
    public class ReadEvents : IReadEvents
    {
        private static DtoRead EMPTY = new DtoRead(new List<ProjectEventItem>(), 0);

        public DtoRead Read(string container, long fromEventId, string? currentUserHash = null, ProjectEventType? filterEventType = null, HashSet<ProjectEventType>? multiFilterEventType = null)
        {
            if (!FileHandles.Exists(container)) return EMPTY;

            var path = container.ContainerPath();

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

            //Step 2 we read all the events from that position to the end of the stream
            var events = new List<ProjectEventItem>();
            long lastServerId = fromEventId;
            while (stream.Position < stream.Length)
            {
                reader.ReadUInt32(); //Version

                var t1 = reader.ReadStringNullable();
                var t2 = reader.ReadStringNullable();
                var t3 = reader.ReadStringNullable();
                var n1 = reader.ReadDoubleNullable();
                var iv = reader.ReadStringNullable();
                var tp = (ProjectEventType)reader.ReadUInt16();
                var ed = reader.ReadInt64();
                var cb = reader.ReadStringNullable();
                lastServerId = reader.ReadInt64();
                reader.ReadInt32(); //totalSize

                //Don't bother creating the object model if we don't want this type of event(s)
                if (filterEventType != null && filterEventType.Value != tp) continue;
                if (multiFilterEventType != null && !multiFilterEventType.Contains(tp)) continue;

                //Don't bother creating the object model if this is the current user
                if (currentUserHash != null && cb == currentUserHash) continue;

                events.Add(new ProjectEventItem(lastServerId, cb, ed, iv, tp, t1, t2, t3, n1));
            }

            return new DtoRead(events, lastServerId);
        }
    }
}
