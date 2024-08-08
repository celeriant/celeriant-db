using Moq;
using System;
using System.Globalization;
using System.Linq;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;
using UtilityDelta.Api.Services;

namespace UtilityDelta.Projects.Tests
{
    [TestClass]
    public class TestEndpoints
    {
        [TestMethod]
        //Don't create the project
        [DataRow(false, ProjectAccess.NotExists)]
        [DataRow(false, ProjectAccess.OwnerAccess)]
        [DataRow(false, ProjectAccess.ReadOnlyAccess)]
        [DataRow(false, ProjectAccess.WriteAccess)]
        [DataRow(false, ProjectAccess.NoAccess)]

        //Scenarios where we create the project
        [DataRow(true, ProjectAccess.OwnerAccess)]
        [DataRow(true, ProjectAccess.ReadOnlyAccess)]
        [DataRow(true, ProjectAccess.WriteAccess)]
        public async Task TestWrite(bool createIfNotExist, ProjectAccess projectAccess)
        {
            var accessLogic = new Mock<IAccessLogic>();
            var readEvents = new Mock<IReadEvents>();
            var writeEvents = new Mock<IWriteAndBackup>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            var userAccessCache = new Mock<IUserAccessCache>();

            var service = new Endpoints(accessLogic.Object, readEvents.Object, writeEvents.Object, shareKeyCache.Object, userAccessCache.Object);

            var pi = "kjlasfd";
            var publicKey = "mykeypub";
            var nonce = "mynonce";
            var sign = "signednonce";

            var events = new List<ProjectEventItem>()
            {
                new ProjectEventItem(0, null, 0, "iv1", ProjectEventType.AddTask, "kljsfddsf", "kjsdfkljd",null, 344)
            }.ToArray();

            accessLogic.Setup(x => x.IsProjectExistAndHasAccess(pi, createIfNotExist, null, publicKey, nonce, sign, CancellationToken.None))
                .Returns(new DtoAccessInfo(projectAccess, publicKey.CalculateHash()));

            var result = await service.Write(pi, publicKey, nonce, sign, createIfNotExist, events, CancellationToken.None);

            switch (projectAccess)
            {
                case ProjectAccess.NoAccess:
                    Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
                    writeEvents.Verify(x => x.WriteClientEvents(events, publicKey.CalculateHash(), pi, CancellationToken.None), Times.Never());
                    break;
                case ProjectAccess.WriteAccess:
                    Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
                    writeEvents.Verify(x => x.WriteClientEvents(events, publicKey.CalculateHash(), pi, CancellationToken.None), Times.Once());
                    break;
                case ProjectAccess.ReadOnlyAccess:
                    Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
                    writeEvents.Verify(x => x.WriteClientEvents(events, publicKey.CalculateHash(), pi, CancellationToken.None), Times.Never());
                    break;
                case ProjectAccess.NotExists:
                    Assert.AreEqual(404, ((Microsoft.AspNetCore.Http.HttpResults.NotFound)result).StatusCode);
                    writeEvents.Verify(x => x.WriteClientEvents(events, publicKey.CalculateHash(), pi, CancellationToken.None), Times.Never());
                    break;
                case ProjectAccess.OwnerAccess:
                    Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
                    writeEvents.Verify(x => x.WriteClientEvents(events, publicKey.CalculateHash(), pi, CancellationToken.None), Times.Once());
                    break;
            }
        }

        [TestMethod]
        //Don't create the project
        [DataRow(false, ProjectAccess.NotExists)]
        [DataRow(false, ProjectAccess.OwnerAccess)]
        [DataRow(false, ProjectAccess.ReadOnlyAccess)]
        [DataRow(false, ProjectAccess.WriteAccess)]
        [DataRow(false, ProjectAccess.NoAccess)]

        //Scenarios where we create the project
        [DataRow(true, ProjectAccess.OwnerAccess)]
        [DataRow(true, ProjectAccess.ReadOnlyAccess)]
        [DataRow(true, ProjectAccess.WriteAccess)]
        public async Task TestRead(bool createIfNotExist, ProjectAccess projectAccess)
        {
            var accessLogic = new Mock<IAccessLogic>();
            var readEvents = new Mock<IReadEvents>();
            var writeEvents = new Mock<IWriteAndBackup>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            var userAccessCache = new Mock<IUserAccessCache>();

            var service = new Endpoints(accessLogic.Object, readEvents.Object, writeEvents.Object, shareKeyCache.Object, userAccessCache.Object);

            var pi = "kjlasfd";
            var publicKey = "mykeypub";
            var nonce = "mynonce";
            var sign = "signednonce";
            var shareKey = "mysharekey";
            var fromTime = createIfNotExist ? 0 : 83432;

            accessLogic.Setup(x => x.IsProjectExistAndHasAccess(pi, createIfNotExist, shareKey, publicKey, nonce, sign, CancellationToken.None))
                .Returns(new DtoAccessInfo(projectAccess, publicKey.CalculateHash()));

            var result = await service.Read(pi, publicKey, nonce, sign, fromTime, createIfNotExist, shareKey, CancellationToken.None);

            switch (projectAccess)
            {
                case ProjectAccess.NoAccess:
                    Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
                    readEvents.Verify(x => x.Read(pi, fromTime, CancellationToken.None, publicKey.CalculateHash(), null, null), Times.Never());

                    break;
                case ProjectAccess.WriteAccess:
                    Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
                    readEvents.Verify(x => x.Read(pi, fromTime, CancellationToken.None, publicKey.CalculateHash(), null, null), Times.Once());

                    break;
                case ProjectAccess.ReadOnlyAccess:
                    Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
                    readEvents.Verify(x => x.Read(pi, fromTime, CancellationToken.None, publicKey.CalculateHash(), null, null), Times.Once());

                    break;
                case ProjectAccess.NotExists:
                    Assert.AreEqual(404, ((Microsoft.AspNetCore.Http.HttpResults.NotFound)result).StatusCode);
                    readEvents.Verify(x => x.Read(pi, fromTime, CancellationToken.None, publicKey.CalculateHash(), null, null), Times.Never());

                    break;
                case ProjectAccess.OwnerAccess:
                    Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
                    readEvents.Verify(x => x.Read(pi, fromTime, CancellationToken.None, publicKey.CalculateHash(), null, null), Times.Once());

                    break;
            }
        }

        [TestMethod]
        //Owners can give others access
        [DataRow(ProjectAccess.OwnerAccess, true, true, 0, false)]
        [DataRow(ProjectAccess.OwnerAccess, false, true, 0, false)]
        [DataRow(ProjectAccess.OwnerAccess, false, false, 0, false)]
        [DataRow(ProjectAccess.OwnerAccess, false, false, 343224, false)]

        //Contributors or readonly can't generate share links at all
        [DataRow(ProjectAccess.WriteAccess, false, false, 0, false)]
        [DataRow(ProjectAccess.ReadOnlyAccess, false, false, 0, true)]
        [DataRow(ProjectAccess.NoAccess, false, false, 0, false)]
        public async Task TestShare(ProjectAccess projectAccess, bool isOwner, bool singleUse, long expiresOn, bool readOnly)
        {
            var accessLogic = new Mock<IAccessLogic>();
            var readEvents = new Mock<IReadEvents>();
            var writeEvents = new Mock<IWriteAndBackup>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            var userAccessCache = new Mock<IUserAccessCache>();

            var service = new Endpoints(accessLogic.Object, readEvents.Object, writeEvents.Object, shareKeyCache.Object, userAccessCache.Object);

            var pi = "kjlasfd";
            var publicKey = "mykeypub";
            var nonce = "mynonce";
            var sign = "signednonce";
            var iv = "test iv";

            accessLogic.Setup(x => x.IsProjectExistAndHasAccess(pi, false, null, publicKey, nonce, sign, CancellationToken.None))
                .Returns(new DtoAccessInfo(projectAccess, publicKey.CalculateHash()));

            var result = await service.Share(pi, publicKey, nonce, sign, isOwner, singleUse, iv, "my desc", expiresOn, readOnly, CancellationToken.None);

            if (projectAccess != ProjectAccess.OwnerAccess)
            {
                shareKeyCache.Verify(x => x.CreateShareLink(pi, It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<long>(), It.IsAny<bool>(), CancellationToken.None), Times.Never);

                Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
            } else
            {
                shareKeyCache.Verify(x => x.CreateShareLink(pi, publicKey.CalculateHash(), isOwner, singleUse, iv, "my desc", expiresOn, readOnly, CancellationToken.None), Times.Once);
                Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok)result).StatusCode);
            }
        }

        [TestMethod]
        [DataRow(ProjectAccess.OwnerAccess)]
        [DataRow(ProjectAccess.WriteAccess)]
        [DataRow(ProjectAccess.ReadOnlyAccess)]
        [DataRow(ProjectAccess.NoAccess)]
        public async Task TestDisableUser(ProjectAccess projectAccess)
        {
            var accessLogic = new Mock<IAccessLogic>();
            var readEvents = new Mock<IReadEvents>();
            var writeEvents = new Mock<IWriteAndBackup>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            var userAccessCache = new Mock<IUserAccessCache>();

            var service = new Endpoints(accessLogic.Object, readEvents.Object, writeEvents.Object, shareKeyCache.Object, userAccessCache.Object);

            var pi = "kjlasfd";
            var publicKey = "mykeypub";
            var nonce = "mynonce";
            var sign = "signednonce";

            accessLogic.Setup(x => x.IsProjectExistAndHasAccess(pi, false, null, publicKey, nonce, sign, CancellationToken.None))
                .Returns(new DtoAccessInfo(projectAccess, publicKey.CalculateHash()));

            var result = await service.DisableUser(pi, publicKey, nonce, sign, "useridtodisableHashed", CancellationToken.None);

            if (projectAccess != ProjectAccess.OwnerAccess)
            {
                userAccessCache.Verify(x => x.UpdateAccess(pi, It.IsAny<string?>(), It.IsAny<string>(), It.IsAny<AccessLevel>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<bool>(), It.IsAny<string?>(), CancellationToken.None), Times.Never);

                Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
            }
            else
            {
                userAccessCache.Verify(x => x.UpdateAccess(pi, publicKey.CalculateHash(), "useridtodisableHashed", null, null, null, true, null, CancellationToken.None), Times.Once);

                Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok<DtoDisableAccess>)result).StatusCode);
            }
        }

        [TestMethod]
        [DataRow(ProjectAccess.OwnerAccess)]
        [DataRow(ProjectAccess.WriteAccess)]
        [DataRow(ProjectAccess.ReadOnlyAccess)]
        [DataRow(ProjectAccess.NoAccess)]
        public async Task TestDisableShare(ProjectAccess projectAccess)
        {
            var accessLogic = new Mock<IAccessLogic>();
            var readEvents = new Mock<IReadEvents>();
            var writeEvents = new Mock<IWriteAndBackup>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            var userAccessCache = new Mock<IUserAccessCache>();

            var service = new Endpoints(accessLogic.Object, readEvents.Object, writeEvents.Object, shareKeyCache.Object, userAccessCache.Object);

            var pi = "kjlasfd";
            var publicKey = "mykeypub";
            var nonce = "mynonce";
            var sign = "signednonce";

            accessLogic.Setup(x => x.IsProjectExistAndHasAccess(pi, false, null, publicKey, nonce, sign, CancellationToken.None))
                .Returns(new DtoAccessInfo(projectAccess, publicKey.CalculateHash()));

            var result = await service.DisableShare(pi, publicKey, nonce, sign, "sharekeyHashed", CancellationToken.None);

            if (projectAccess != ProjectAccess.OwnerAccess)
            {
                shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, It.IsAny<string?>(), It.IsAny<string>(), CancellationToken.None), Times.Never);

                Assert.AreEqual(403, ((Microsoft.AspNetCore.Http.HttpResults.StatusCodeHttpResult)result).StatusCode);
            }
            else
            {
                shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, publicKey.CalculateHash(), "sharekeyHashed", CancellationToken.None), Times.Once);

                Assert.AreEqual(200, ((Microsoft.AspNetCore.Http.HttpResults.Ok<DtoDisableAccess>)result).StatusCode);
            }
        }
    }
}
