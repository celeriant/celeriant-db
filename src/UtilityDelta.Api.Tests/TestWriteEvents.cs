using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Tests
{
    [TestClass]
    public class TestWriteEvents
    {
        [TestMethod]
        public void TestWriteNoFileHandlesManager()
        {
            if (Directory.Exists(nameof(TestWriteNoFileHandlesManager))) Directory.Delete(nameof(TestWriteNoFileHandlesManager), true);
            Directory.CreateDirectory(nameof(TestWriteNoFileHandlesManager));
            var fileHandlesManager = new Mock<IFileHandlesManager>();

            var containerName = "test";
            using var fileStream = new FileStream($"{nameof(TestWriteNoFileHandlesManager)}\\{containerName}", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            var _containerFileStreams = new ConcurrentDictionary<string, FileStream>();
            var _isInUse = new ConcurrentDictionary<string, int>();

            fileHandlesManager.Setup(x => x.OpenWrite(containerName)).Returns(new FileHandles(fileStream, containerName, _containerFileStreams, _isInUse, (x, y) => true));
            var service = new WriteEvents(fileHandlesManager.Object);

            var cb = "tyson";
            var events = new ProjectEventItem[]
            {

            };
            service.WriteClientEvents(events, cb, containerName, CancellationToken.None);

            //Didn't write anything
            Assert.AreEqual(0, fileStream.Position);

            events = new ProjectEventItem[]
            {
                new ProjectEventItem(0, null, 0, null, ProjectEventType.AddItemToStandup, null, null, null, null)
            };
            var r1 = service.WriteClientEvents(events, cb, containerName, CancellationToken.None);
            Assert.AreEqual(1, r1.serverId);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r1.eventDate < 2);

            //version (4), 4 data markers (4), iv marker (1) type (2), time (8), marker (1), strsize prefix (1), createdby (str len), id (8), write len (4)
            var cbLen = Encoding.UTF8.GetBytes(cb).Length;
            var totalLen = 4 + 4 + 1 + 2 + 8 + 1 + 1 + cbLen + 8 + 4;
            Assert.AreEqual(totalLen, fileStream.Length);

            //Write another event, this time with some data
            var cb2 = "jacinta";
            events = new ProjectEventItem[]
            {
                new ProjectEventItem(0, null, 0, "IV", ProjectEventType.AddRole, "T1", "T2", "T3", 99.232),
                
                //Test we can't write server events via WriteClientEvents
                new ProjectEventItem(0, null, 0, null, ProjectEventType.AddShareLink, null, null, null, null),
                new ProjectEventItem(0, null, 0, null, ProjectEventType.AddSingleUseShareLink, null, null, null, null),
                new ProjectEventItem(0, null, 0, null, ProjectEventType.ProvideAccess, null, null, null, null),
                new ProjectEventItem(0, null, 0, null, ProjectEventType.DisableShareLink, null, null, null, null),
            };
            var r2 = service.WriteClientEvents(events, cb2, containerName, CancellationToken.None);
            Assert.AreEqual(2, r2.serverId);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r2.eventDate < 2);

            //version (4), 4 data markers (4), 3xstrings of 2byte+1byte len, n1 (8byte double),
            //iv marker (1) + 2byte IV, type (2), time (8), marker (1), strsize prefix (1), createdby (str len), id (8), write len (4)
            var cb2Len = Encoding.UTF8.GetBytes(cb2).Length;
            var totalLen2 = 4 + 4 + 3*3 + 8 + 1 + 3 + 2 + 8 + 1 + 1 + cb2Len + 8 + 4;

            Assert.AreEqual(totalLen + totalLen2, fileStream.Length);
        }

        [TestMethod]
        public void TestCurrentVersion()
        {
            if (Directory.Exists(nameof(TestCurrentVersion))) Directory.Delete(nameof(TestCurrentVersion), true);

            var utilityDeltaConfiguration = new Mock<IOptions<SystemSettings>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new SystemSettings()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = nameof(TestCurrentVersion)
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var writeEvents = new WriteEvents(fileHandlesManager);

            var cb = "tyson";
            var pi1 = "pi1";
            var r1 = writeEvents.WriteClientEvents(new ProjectEventItem[]
            {
                new ProjectEventItem(0, null, 0, null, ProjectEventType.AddTask, "task1", null, null, 12.12),
                new ProjectEventItem(0, null, 0, null, ProjectEventType.SetParent, null, "parent1", null, null),
            }, cb, pi1, CancellationToken.None);
            Assert.AreEqual(2, r1.serverId);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r1.eventDate == 0);

            var r2 = writeEvents.WriteServerEvent(new ProjectEventItem(0, null, 0, null, ProjectEventType.AddShareLink, "sdf", "sharelink", null, 123.12), pi1);
            Assert.AreEqual(3, r2.serverId);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r2.ed < 2);

            var cb2 = "frank";
            var r3 = writeEvents.WriteClientEvents(new ProjectEventItem[]
            {
                new ProjectEventItem(0, null, 0, null, ProjectEventType.AddTask, null, null, "task2", 111),
            }, cb2, pi1, CancellationToken.None);
            Assert.AreEqual(4, r3.serverId);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r3.eventDate == 0);

            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r4 = readEvents.Read(pi1, 0, CancellationToken.None, null, null, null);
            V1Assertions(cb, cb2, r4, 0);

            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r4.events[0].ed == 0);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r4.events[1].ed == 0);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r4.events[2].ed == 0);
            Assert.IsTrue(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - r4.events[3].ed == 0);
        }

        public static void V1Assertions(string cb, string cb2, DtoRead r4, int skip)
        {
            Assert.AreEqual(4, r4.serverId);

            if (skip <= 0) Assert.AreEqual(ProjectEventType.AddTask, r4.events[0 - skip].tp);
            if (skip <= 1) Assert.AreEqual(ProjectEventType.SetParent, r4.events[1-skip].tp);
            if (skip <= 2) Assert.AreEqual(ProjectEventType.AddShareLink, r4.events[2 - skip].tp);
            Assert.AreEqual(ProjectEventType.AddTask, r4.events[3 - skip].tp);

            if (skip <= 0) Assert.AreEqual("task1", r4.events[0 - skip].t1);
            if (skip <= 1) Assert.AreEqual("parent1", r4.events[1 - skip].t2);
            if (skip <= 2) Assert.AreEqual("sdf", r4.events[2 - skip].t1);
            if (skip <= 2) Assert.AreEqual("sharelink", r4.events[2 - skip].t2);
            Assert.AreEqual("task2", r4.events[3 - skip].t3);

            if (skip <= 0) Assert.AreEqual(12.12, r4.events[0 - skip].n1);
            if (skip <= 1) Assert.AreEqual(null, r4.events[1 - skip].n1);
            if (skip <= 2) Assert.AreEqual(123.12, r4.events[2 - skip].n1);
            Assert.AreEqual(111, r4.events[3 - skip].n1);

            if (skip <= 0) Assert.AreEqual(1, r4.events[0 - skip].serverId);
            if (skip <= 1) Assert.AreEqual(2, r4.events[1 - skip].serverId);
            if (skip <= 2) Assert.AreEqual(3, r4.events[2 - skip].serverId);
            Assert.AreEqual(4, r4.events[3 - skip].serverId);

            if (skip <= 0) Assert.AreEqual(cb, r4.events[0 - skip].cb);
            if (skip <= 1) Assert.AreEqual(cb, r4.events[1 - skip].cb);
            if (skip <= 2) Assert.AreEqual(null, r4.events[2 - skip].cb);
            Assert.AreEqual(cb2, r4.events[3 - skip].cb);
        }
    }
}
