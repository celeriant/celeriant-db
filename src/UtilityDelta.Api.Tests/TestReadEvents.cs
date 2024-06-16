using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UtilityDelta.Api.Services;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Tests
{
    [TestClass]
    public class TestReadEvents
    {
        [TestMethod]
        public void TestV1()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestV1), 0, CancellationToken.None);
            Assert.AreEqual(4, r1.events.Count);

            TestWriteEvents.V1Assertions("tyson", "frank", r1, 0);
        }

        [TestMethod]
        public void TestFilterSelfEvents()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestV1), 0, CancellationToken.None, currentUserHash: "tyson");
            Assert.AreEqual(2, r1.events.Count);

            TestWriteEvents.V1Assertions("tyson", "frank", r1, 2);

            var r2 = readEvents.Read(nameof(TestV1), 0, CancellationToken.None, currentUserHash: "frank");
            Assert.AreEqual(3, r2.events.Count);
            Assert.IsFalse(r2.events.Any(x => x.cb == "frank"));

        }

        [TestMethod]
        public void TestLimitToServerId()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestV1), 2, CancellationToken.None);
            Assert.AreEqual(2, r1.events.Count);

            TestWriteEvents.V1Assertions("tyson", "frank", r1, 2);
        }

        [TestMethod]
        public void TestLimitToEventType()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestV1), 0, CancellationToken.None, "tyson", ProjectEventType.AddTask);
            Assert.AreEqual(1, r1.events.Count);

            TestWriteEvents.V1Assertions("tyson", "frank", r1, 3);
        }

        [TestMethod]
        public void TestLimitToEventTypes()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestV1), 1, CancellationToken.None, null, null, new HashSet<ProjectEventType>() { ProjectEventType.AddTask, ProjectEventType.AddShareLink});
            Assert.AreEqual(2, r1.events.Count);

            TestWriteEvents.V1Assertions("tyson", "frank", r1, 2);
        }

        [TestMethod]
        public void TestNotFound()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty
            });

            var fileHandlesManager = new FileHandlesManager(utilityDeltaConfiguration.Object);
            var readEvents = new ReadEvents(utilityDeltaConfiguration.Object, fileHandlesManager);
            var r1 = readEvents.Read(nameof(TestNotFound), 2, CancellationToken.None);

            Assert.AreEqual(0, r1.events.Count);
            Assert.AreEqual(0, r1.serverId);
        }
    }
}
