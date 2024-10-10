using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.Projects.Tests
{
    [TestClass]
    public class TestAccessLogic
    {        
        [TestMethod]

        //Exists and not exists scenarios
        [DataRow(false, false, false)]
        [DataRow(true, false, false)]

        //Project exists, but no access, share key invalid
        [DataRow(true, true, false)]

        //Project exists, but no access, valid share key, but on 'claiming' of share key it fails (someone got there first)
        [DataRow(true, true, true)]
        public void NotExistsDoNotCreateOrExistsNoAccess(bool exists, bool shareLinkProvidedButWrong, bool sharekeyClaimFailed)
        {
            var pi = "myproject1";
            var fileHandlesManager = new Mock<IFileHandlesManager>();
            fileHandlesManager.Setup(x => x.Exists(pi)).Returns(exists); 
            var crypto = new Mock<ICrypto>();
            var userAccessCache = new Mock<IUserAccessCache>();
            var shareKeyCache = new Mock<IShareKeyCache>();
            if (sharekeyClaimFailed)
            {
                shareKeyCache.Setup(x => x.GetShareKeyDataIfStillValid(pi, "wrongsharekey".CalculateHash(), CancellationToken.None))
                    .Returns(new DtoShareKeyData(null, AccessLevel.Owner, null, null, "kljsdfj", true, "kjlf"));
                shareKeyCache.Setup(x => x.MarkShareKeyAsUsed(pi, null, "wrongsharekey".CalculateHash(), CancellationToken.None))
                    .Returns((ProjectEventItem?)null);
            }

            var service = new AccessLogic(fileHandlesManager.Object, crypto.Object, userAccessCache.Object, shareKeyCache.Object);

            var result = service.IsProjectExistAndHasAccess(pi, false, shareLinkProvidedButWrong ? "wrongsharekey" : null, "publicKey", "nonce", "sign", CancellationToken.None);

            Assert.AreEqual(exists ? ProjectAccess.NoAccess : ProjectAccess.NotExists, result.ProjectAccess);
            Assert.AreEqual("publicKey".CalculateHash(), result.CurrentUserHash);

            if (shareLinkProvidedButWrong && !sharekeyClaimFailed)
            {
                shareKeyCache.Verify(x => x.GetShareKeyDataIfStillValid(pi, "wrongsharekey".CalculateHash(), CancellationToken.None), Times.Once());
            }
            if (sharekeyClaimFailed)
            {
                shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, null, "wrongsharekey".CalculateHash(), CancellationToken.None), Times.Once);
            }
            crypto.Verify(x => x.ValidateWithPublicKey("publicKey", "nonce", "sign"), Times.Once);
            userAccessCache.Verify(x => x.UpdateAccess(pi, null, result.CurrentUserHash, AccessLevel.Owner, null, "Project creator", false, null, null, CancellationToken.None), Times.Never);

        }

        [TestMethod]
        public void NotExistsDoCreate()
        {
            var pi = "myproject1";
            var fileHandlesManager = new Mock<IFileHandlesManager>();
            fileHandlesManager.Setup(x => x.Exists(pi)).Returns(false);
            var crypto = new Mock<ICrypto>();
            var userAccessCache = new Mock<IUserAccessCache>();
            var shareKeyCache = new Mock<IShareKeyCache>();

            var service = new AccessLogic(fileHandlesManager.Object, crypto.Object, userAccessCache.Object, shareKeyCache.Object);

            var result = service.IsProjectExistAndHasAccess(pi, true, null, "publicKey", "nonce", "sign", CancellationToken.None);

            Assert.AreEqual(ProjectAccess.OwnerAccess, result.ProjectAccess);
            Assert.AreEqual("publicKey".CalculateHash(), result.CurrentUserHash);

            //Ensure this is a system created event (cb is null)
            crypto.Verify(x => x.ValidateWithPublicKey("publicKey", "nonce", "sign"), Times.Once);
            userAccessCache.Verify(x => x.UpdateAccess(pi, null, result.CurrentUserHash, AccessLevel.Owner, null, "Project creator", false, null, null, CancellationToken.None), Times.Once);
        }

        //Single use share key, attempted to be used by creator, ignore, share key still valid
        [TestMethod]
        public void UseOwnShareKeyNotUsedUp()
        {
            var pi = "myproject1";
            var cb = "publicKey".CalculateHash();
            var shareKey = "mysharekey";
            var shareKeyHash = shareKey.CalculateHash();

            var fileHandlesManager = new Mock<IFileHandlesManager>();
            var crypto = new Mock<ICrypto>();
            var userAccessCache = new Mock<IUserAccessCache>();
            var shareKeyCache = new Mock<IShareKeyCache>();

            userAccessCache.Setup(x => x.GetCurrentAccess(pi, cb, CancellationToken.None)).Returns(AccessLevel.Owner);
            fileHandlesManager.Setup(x => x.Exists(pi)).Returns(true);
            shareKeyCache.Setup(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None))
                .Returns(new DtoShareKeyData(null, AccessLevel.Owner, null, null, shareKeyHash, true, cb));

            var service = new AccessLogic(fileHandlesManager.Object, crypto.Object, userAccessCache.Object, shareKeyCache.Object);

            var result = service.IsProjectExistAndHasAccess(pi, false, shareKey, "publicKey", "nonce", "sign", CancellationToken.None);

            Assert.AreEqual(ProjectAccess.OwnerAccess, result.ProjectAccess);
            Assert.AreEqual(cb, result.CurrentUserHash);

            shareKeyCache.Verify(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None), Times.Once());
            shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, null, shareKeyHash, CancellationToken.None), Times.Never);

            crypto.Verify(x => x.ValidateWithPublicKey("publicKey", "nonce", "sign"), Times.Once);
            userAccessCache.Verify(x => x.UpdateAccess(
                pi, It.IsAny<string?>(), It.IsAny<string>(), It.IsAny<AccessLevel>(), 
                null, It.IsAny<string?>(), It.IsAny<bool>(), It.IsAny<string?>(), null, CancellationToken.None), Times.Never);

        }

        //Project exists, user has access, Attempt to use share key, but current user already has that access level. No event created (ignore share key)
        [TestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void ShareKeyNotRequired(bool providedShareKey)
        {
            var pi = "myproject1";
            var cb = "publicKey".CalculateHash();
            var shareKey = "mysharekey";
            var shareKeyHash = shareKey.CalculateHash();

            var fileHandlesManager = new Mock<IFileHandlesManager>();
            var crypto = new Mock<ICrypto>();
            var userAccessCache = new Mock<IUserAccessCache>();
            var shareKeyCache = new Mock<IShareKeyCache>();

            userAccessCache.Setup(x => x.GetCurrentAccess(pi, cb, CancellationToken.None)).Returns(AccessLevel.Contributor);
            fileHandlesManager.Setup(x => x.Exists(pi)).Returns(true);
            shareKeyCache.Setup(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None))
                .Returns(new DtoShareKeyData(null, AccessLevel.Viewer, null, null, shareKeyHash, true, "anotheruserhash"));

            var service = new AccessLogic(fileHandlesManager.Object, crypto.Object, userAccessCache.Object, shareKeyCache.Object);

            var result = service.IsProjectExistAndHasAccess(pi, true, providedShareKey ? shareKey : null, "publicKey", "nonce", "sign", CancellationToken.None);

            Assert.AreEqual(ProjectAccess.WriteAccess, result.ProjectAccess);
            Assert.AreEqual(cb, result.CurrentUserHash);

            if (providedShareKey)
            {
                shareKeyCache.Verify(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None), Times.Once());

                //As its not own share key we must expire it even though it provides no extra access
                shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, null, shareKeyHash, CancellationToken.None), Times.Once());
            }

            crypto.Verify(x => x.ValidateWithPublicKey("publicKey", "nonce", "sign"), Times.Once);
            userAccessCache.Verify(x => x.UpdateAccess(
                pi, It.IsAny<string?>(), It.IsAny<string>(), It.IsAny<AccessLevel>(),
                 null, It.IsAny<string?>(), It.IsAny<bool>(), It.IsAny<string?>(), null, CancellationToken.None), Times.Never);

        }

        //Project exists, user has access, Attempt to use share key, but current user already has that access level. No event created (ignore share key)
        [TestMethod]
        public void ShareKeyRequired()
        {
            var pi = "myproject1";
            var cb = "publicKey".CalculateHash();
            var shareKey = "mysharekey";
            var shareKeyHash = shareKey.CalculateHash();

            var fileHandlesManager = new Mock<IFileHandlesManager>();
            var crypto = new Mock<ICrypto>();
            var userAccessCache = new Mock<IUserAccessCache>();
            var shareKeyCache = new Mock<IShareKeyCache>();

            userAccessCache.Setup(x => x.GetCurrentAccess(pi, cb, CancellationToken.None)).Returns((AccessLevel?)null);
            fileHandlesManager.Setup(x => x.Exists(pi)).Returns(true);
            shareKeyCache.Setup(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None))
                .Returns(new DtoShareKeyData(null, AccessLevel.Viewer, null, null, shareKeyHash, true, "anotheruserhash"));
            shareKeyCache.Setup(x => x.MarkShareKeyAsUsed(pi, null, shareKeyHash, CancellationToken.None)).Returns(new ProjectEventItem(0,null,0,null, ProjectEventType.AddTask, null, null, null, null));

            var service = new AccessLogic(fileHandlesManager.Object, crypto.Object, userAccessCache.Object, shareKeyCache.Object);

            var result = service.IsProjectExistAndHasAccess(pi, false, shareKey, "publicKey", "nonce", "sign", CancellationToken.None);

            Assert.AreEqual(ProjectAccess.ReadOnlyAccess, result.ProjectAccess);
            Assert.AreEqual(cb, result.CurrentUserHash);

            shareKeyCache.Verify(x => x.GetShareKeyDataIfStillValid(pi, shareKeyHash, CancellationToken.None), Times.Once());

            //As its not own share key we must expire it even though it provides no extra access
            shareKeyCache.Verify(x => x.MarkShareKeyAsUsed(pi, null, shareKeyHash, CancellationToken.None), Times.Once());

            crypto.Verify(x => x.ValidateWithPublicKey("publicKey", "nonce", "sign"), Times.Once);

            userAccessCache.Verify(x => x.UpdateAccess(
                pi, null, cb, AccessLevel.Viewer, null, null, false, shareKeyHash, null, CancellationToken.None), Times.Once);

        }

    }
}
