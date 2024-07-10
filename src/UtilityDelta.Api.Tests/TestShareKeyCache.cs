using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Linq;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Services;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.Tests
{
    [TestClass]
    public class TestShareKeyCache
    {
        [TestMethod]
        [DataRow(true, false, false, false, 2)]
        [DataRow(false, false, false, false, 2)]
        [DataRow(false, true, false, false, 2)]
        [DataRow(true, true, false, false, 2)]
        [DataRow(false, true, true, true, 2)]
        [DataRow(false, true, true, true, 1)]
        public void Test1(
            bool isOwner,
            bool isSingleUse,
            bool expiresOn,
            bool readOnly,
            int CACHE_MAX_PROJECT_COUNT)
        {
            string? description = "test description";
            string? iv = "test iv";

            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty,
                CACHE_CHECK_TIME_HOURS = 0,
                CACHE_MAX_PROJECT_COUNT = CACHE_MAX_PROJECT_COUNT,
                CACHE_MAX_SHARE_LINKS_PER_PROJECT = 3,
                CACHE_MAX_USERS_PER_PROJECT = 3
            });

            var writeEvents = new Mock<IWriteAndBackup>();
            var readEvents = new Mock<IReadEvents>();
            var service = new ShareKeyCache(writeEvents.Object, readEvents.Object, utilityDeltaConfiguration.Object);

            var pi = "testProject";

            //The cache will be built for the project
            readEvents!.Setup(x => x.Read(
                pi, 0, CancellationToken.None, null, null,
                It.Is<HashSet<ProjectEventType>>(y => y.Count == 3 && y.Contains(ProjectEventType.AddShareLink) && y.Contains(ProjectEventType.AddSingleUseShareLink) && y.Contains(ProjectEventType.DisableShareLink))))
                .Returns(new DtoRead([], 6));

            var expiresOnLong = expiresOn ? DateTime.UtcNow.AddSeconds(20).ToUnixTimeSeconds() : 0;

            var tp = isSingleUse ? ProjectEventType.AddSingleUseShareLink : ProjectEventType.AddShareLink;
            var accessLevel = isOwner ? AccessLevel.Owner : readOnly ? AccessLevel.Viewer : AccessLevel.Contributor;
            var shareEvent = new ProjectEventItem(0, "tyson", 0, iv, tp, t1: description, t2: accessLevel.ToString(), t3: "hashedCode", n1: expiresOnLong > 0 ? expiresOnLong : null);

            //The event is written to the stream
            writeEvents!.Setup(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y =>
                y.n1 == shareEvent.n1 &&
                y.cb == shareEvent.cb &&
                y.tp == shareEvent.tp &&
                y.t1 == shareEvent.t1 &&
                y.t2 == shareEvent.t2 &&
                y.t3 != null
                ), pi)).Returns(shareEvent);

            var result = service!.CreateShareLink(pi, "tyson", isOwner, isSingleUse, iv, description, expiresOnLong, readOnly, CancellationToken.None);

            Assert.AreEqual(shareEvent, result.shareEvent);
            Assert.IsNotNull(result.shareKey);

            GetKeyDataAndAssert(isSingleUse, description, readEvents, service, pi, expiresOnLong, accessLevel, result);

            //Disable logic - try to disable but wrong key
            var resultDisable1 = service.MarkShareKeyAsUsed(pi, null, "some other key", CancellationToken.None);
            Assert.IsFalse(resultDisable1 != null);

            //Assert we did not write an event
            writeEvents.Verify(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink), pi), Times.Never);

            GetKeyDataAndAssert(isSingleUse, description, readEvents, service, pi, expiresOnLong, accessLevel, result);

            //Check a call to either 3 functions will trigger cache load for new project, this won't affect previous project cache as our limit is 2
            readEvents!.Setup(x => x.Read(
                "some other project", 0, CancellationToken.None, null, null,
                It.Is<HashSet<ProjectEventType>>(y => y.Count == 3 && y.Contains(ProjectEventType.AddShareLink) && y.Contains(ProjectEventType.AddSingleUseShareLink) && y.Contains(ProjectEventType.DisableShareLink))))
                .Returns(new DtoRead([], 6));
            _ = service.MarkShareKeyAsUsed("some other project", null, result.shareKey.CalculateHash(), CancellationToken.None);

            writeEvents.Setup(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink && y.t1 == result.shareKey.CalculateHash()), pi))
                .Returns(new ProjectEventItem(0, null, 0, null, ProjectEventType.AddItemToStandup, null, null, null, null));

            //Disable the link - should disable in the cache and write an event
            var resultDisable2 = service.MarkShareKeyAsUsed(pi, null, result.shareKey.CalculateHash(), CancellationToken.None);

            if (CACHE_MAX_PROJECT_COUNT == 1)
            {
                //Triggers a cache reload, and our mock returns no share events, so disable returns failure
                Assert.IsFalse(resultDisable2 != null);
                return;
            }

            Assert.IsTrue(resultDisable2 != null);
            writeEvents.Verify(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink && y.t1 == result.shareKey.CalculateHash()), pi), Times.Once);

            var keyDataNone = service.GetShareKeyDataIfStillValid(pi, result.shareKey.CalculateHash(), CancellationToken.None);
            Assert.IsNull(keyDataNone);
        }


        private static void GetKeyDataAndAssert(bool isSingleUse, string? description, Mock<IReadEvents> readEvents, ShareKeyCache service, string pi, long expiresOnLong, AccessLevel accessLevel, DtoShare result)
        {

            //Now we can check if the key is in the cache by requesting it
            var keyData = service.GetShareKeyDataIfStillValid(pi, result.shareKey!.CalculateHash(), CancellationToken.None);
            Assert.IsNotNull(keyData);

            var expiresOnDateTime = expiresOnLong > 0 ? (DateTime?)expiresOnLong.FromUnixTimeSeconds() : null;
            Assert.AreEqual(expiresOnDateTime, keyData.expiresOn);
            Assert.AreEqual(accessLevel, keyData.accessLevel);
            Assert.AreEqual(description, keyData.description);
            Assert.AreEqual(result.shareKey!.CalculateHash(), keyData.hashedCode);
            Assert.AreEqual(isSingleUse, keyData.isSingleUse);
            Assert.AreEqual("tyson", keyData.createdBy);

            //Hit the cache, don't read a second time
            readEvents.Verify(x => x.Read(pi, 0, CancellationToken.None, null, null,
                It.Is<HashSet<ProjectEventType>>(y => y.Count == 3 && y.Contains(ProjectEventType.AddShareLink) && y.Contains(ProjectEventType.AddSingleUseShareLink) && y.Contains(ProjectEventType.DisableShareLink))), Times.Once);
        }

        [TestMethod]
        public void TestPopulateCache()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty,
                CACHE_CHECK_TIME_HOURS = 0,
                CACHE_MAX_PROJECT_COUNT = 2,
                CACHE_MAX_SHARE_LINKS_PER_PROJECT = 3,
                CACHE_MAX_USERS_PER_PROJECT = 3
            });

            var writeEvents = new Mock<IWriteAndBackup>();
            var readEvents = new Mock<IReadEvents>();
            var service = new ShareKeyCache(writeEvents.Object, readEvents.Object, utilityDeltaConfiguration.Object);

            var pi = "testProject";

            //The cache will be built for the project
            readEvents!.Setup(x => x.Read(
                pi, 0, CancellationToken.None, null, null,
                It.Is<HashSet<ProjectEventType>>(y => y.Count == 3 && y.Contains(ProjectEventType.AddShareLink) && y.Contains(ProjectEventType.AddSingleUseShareLink) && y.Contains(ProjectEventType.DisableShareLink))))
                .Returns(new DtoRead(new List<ProjectEventItem>()
                {
                    new ProjectEventItem(3, "tyson", 44, "someiv1", ProjectEventType.AddShareLink, "my link", AccessLevel.Owner.ToString(), "hashedcode1", DateTime.UtcNow.AddDays(1).ToUnixTimeSeconds()),
                    new ProjectEventItem(8, "tyson", 49, "someiv2", ProjectEventType.AddSingleUseShareLink, "my link2", AccessLevel.Contributor.ToString(), "hashedcode2", null),
                    new ProjectEventItem(14, "frank", 88, null, ProjectEventType.DisableShareLink, "hashedcode2", null, null, null),
                }, 17));


            var resultDisable1 = service.MarkShareKeyAsUsed(pi, null, "hashedcode2", CancellationToken.None);
            Assert.IsFalse(resultDisable1 != null);
            writeEvents.Verify(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink), pi), Times.Never);

            writeEvents.Setup(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink && y.t1 == "hashedcode1"), pi))
                .Returns(new ProjectEventItem(0,null,0,null,ProjectEventType.AddItemToStandup,null,null,null,null));

            var resultDisable2 = service.MarkShareKeyAsUsed(pi, null, "hashedcode1", CancellationToken.None);
            Assert.IsTrue(resultDisable2 != null);
            writeEvents.Verify(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => y.tp == ProjectEventType.DisableShareLink && y.t1 == "hashedcode1"), pi), Times.Once);

            //Hit the cache, don't read a second time
            readEvents.Verify(x => x.Read(pi, 0, CancellationToken.None, null, null,
                It.Is<HashSet<ProjectEventType>>(y => y.Count == 3 && y.Contains(ProjectEventType.AddShareLink) && y.Contains(ProjectEventType.AddSingleUseShareLink) && y.Contains(ProjectEventType.DisableShareLink))), Times.Once);
        }
    }
}
