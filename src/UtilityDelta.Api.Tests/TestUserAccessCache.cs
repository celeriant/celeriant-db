using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Tests
{
    [TestClass]
    public class TestUserAccessCache
    {
        [TestMethod]
        public void Test1()
        {
            var pi1 = "p1";
            var cu1 = "c1";
            var iv = "test iv";

            var utilityDeltaConfiguration = new Mock<IOptions<SystemSettings>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new SystemSettings()
            {
                FILE_HANDLE_OPEN_LIMIT = 10,
                SUB_DIR_CONTAINERS = string.Empty,
                CACHE_CHECK_TIME_HOURS = 0,
                CACHE_MAX_PROJECT_COUNT = 1,
                CACHE_MAX_SHARE_LINKS_PER_PROJECT = 3,
                CACHE_MAX_USERS_PER_PROJECT = 3
            });

            var writeEvents = new Mock<IWriteAndBackup>();
            var readEvents = new Mock<IReadEvents>();
            readEvents.Setup(x => x.Read(pi1, 0, CancellationToken.None, null, ProjectEventType.ProvideAccess, null)).Returns(new DtoRead([], 0));

            var service = new UserAccessCache(writeEvents.Object, readEvents.Object, utilityDeltaConfiguration.Object);

            var accessLevel = service.GetCurrentAccess(pi1, "foruser1", CancellationToken.None);
            Assert.IsNull(accessLevel);

            readEvents.Setup(x => x.Read(pi1, 0, CancellationToken.None, null, ProjectEventType.ProvideAccess, null)).Returns(new DtoRead(new List<ProjectEventItem>()
            {
                new ProjectEventItem(2, cu1, 44, null, ProjectEventType.ProvideAccess, "desc here", "foruser1", "sharekey1", (double?)AccessLevel.Contributor),
                new ProjectEventItem(43, cu1, 55, null, ProjectEventType.ProvideAccess, "desc here", "foruser1", "sharekey1", null),
                new ProjectEventItem(45, cu1, 66, null, ProjectEventType.ProvideAccess, "desc here", "foruser1", "sharekey1", (double?)AccessLevel.Viewer)
            }, 0));

            //Cached - so create new service
            service = new UserAccessCache(writeEvents.Object, readEvents.Object, utilityDeltaConfiguration.Object);
            accessLevel = service.GetCurrentAccess(pi1, "foruser1", CancellationToken.None);
            Assert.AreEqual(AccessLevel.Viewer, accessLevel);

            //Try to lower own access - error
            var eventResult = service.UpdateAccess(pi1, "foruser1", "foruser1", null, iv, "desc here", allowDowngrade: true, shareKey: null, null, CancellationToken.None);
            Assert.IsNull(eventResult);

            writeEvents.Setup(x => x.WriteServerEvent(It.Is<ProjectEventItem>(y => 
                y.cb == cu1 &&
                y.ed == 0 &&
                y.serverId == 0 &&
                y.tp == ProjectEventType.ProvideAccess &&
                y.t1 == "desc here" &&
                y.t2 == "foruser1" &&
                y.t3 == "sharekey1" &&
                y.n1 == (double?)AccessLevel.Contributor
                ), pi1))
                .Returns(new ProjectEventItem(2, cu1, 999, null, ProjectEventType.ProvideAccess, "desc here", "foruser1", "sharekey1", (double?)AccessLevel.Contributor));

            eventResult = service.UpdateAccess(pi1, cu1, "foruser1", AccessLevel.Contributor, iv, "desc here", allowDowngrade: true, shareKey: "sharekey1", null, CancellationToken.None);
            Assert.IsNotNull(eventResult);

            accessLevel = service.GetCurrentAccess(pi1, "foruser1", CancellationToken.None);
            Assert.AreEqual(AccessLevel.Contributor, accessLevel);

            //Try downgrade without flag set
            eventResult = service.UpdateAccess(pi1, cu1, "foruser1", AccessLevel.Viewer, iv, "desc here", allowDowngrade: false, shareKey: "sharekey1", null, CancellationToken.None);
            Assert.IsNull(eventResult);
        }
    }
}
