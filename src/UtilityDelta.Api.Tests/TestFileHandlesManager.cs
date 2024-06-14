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
    public class TestFileHandlesManager
    {
        [TestMethod]
        public void TestZeroOpenLimit()
        {
            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 0
            });
            var service = new FileHandlesManager(utilityDeltaConfiguration.Object);

            var test1 = service.OpenWrite("test1");
            var test2 = service.OpenWrite("test2");
        }
    }
}
