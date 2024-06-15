using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
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
            if (Directory.Exists("testfolder")) Directory.Delete("testfolder", true);

            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = 1,
                SUB_DIR_CONTAINERS = "testfolder"
            });
            var service = new FileHandlesManager(utilityDeltaConfiguration.Object);

            var test1a = service.OpenWrite("test1");
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test1"));

            var test1b = service.OpenWrite("test1");
            Assert.AreEqual(2, service.NumberOfConnectionsToContainer("test1"));

            var test2 = service.OpenWrite("test2");
            Assert.AreEqual(2, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(2, service.NumberOfOpenStreams);

            //We should only have one stream open at a time for a container
            Assert.IsTrue(test1a.Stream == test1b.Stream);

            //Different container allows different stream
            Assert.IsTrue(test1a.Stream != test2.Stream);

            //Ensure the folder is created
            //Files created 
            Assert.IsTrue(Directory.Exists("testfolder"));
            Assert.IsTrue(File.Exists("testfolder\\test1"));
            Assert.IsTrue(File.Exists("testfolder\\test2"));

            //Now write some data
            using (var writer = new StreamWriter(test1a.Stream, Encoding.UTF8, leaveOpen: true))
            {
                writer.WriteLine("test!");
            }
            test1a.Dispose(); //Simulate dispose of first, but can still use second to write

            using (var writer = new StreamWriter(test2.Stream, Encoding.UTF8, leaveOpen: true))
            {
                writer.WriteLine("stream 2");
            }

            using (var writer = new StreamWriter(test1b.Stream, Encoding.UTF8, leaveOpen: true))
            {
                writer.WriteLine("another line");
            }
            test1b.Dispose();

            //Check we can open and read the file even though we are still writing to it
            using var stream = new FileStream("testfolder\\test2", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
            
            Assert.AreEqual("stream 2", reader.ReadLine());

            //And try another read-write combo
            using (var writer = new StreamWriter(test2.Stream, Encoding.UTF8, leaveOpen: true))
            {
                writer.WriteLine("stream 2 again");
            }

            Assert.AreEqual("stream 2 again", reader.ReadLine());

            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(2, service.NumberOfOpenStreams);

            var test1c = service.OpenWrite("test1");

            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(2, service.NumberOfOpenStreams);

            test1c.Dispose();

            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(2, service.NumberOfOpenStreams);

            //Now try to open test3, should remove the stream for test1
            var test3 = service.OpenWrite("test3");

            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test3"));
            Assert.AreEqual(2, service.NumberOfOpenStreams);

            var test1d = service.OpenWrite("test1");

            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test3"));
            Assert.AreEqual(3, service.NumberOfOpenStreams);

            test1d.Dispose();
            test2.Dispose();
            test3.Dispose();

            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test3"));
            Assert.AreEqual(3, service.NumberOfOpenStreams);

            var test1e = service.OpenWrite("test1");

            Assert.AreEqual(1, service.NumberOfConnectionsToContainer("test1"));
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test2"));
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer("test3"));
            Assert.AreEqual(1, service.NumberOfOpenStreams);
        }

        [TestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        public void OpenStreamsTest(int FILE_HANDLE_OPEN_LIMIT)
        {
            var folder = "OpenStreamsTest" + FILE_HANDLE_OPEN_LIMIT;
            if (Directory.Exists(folder)) Directory.Delete(folder, true);

            var utilityDeltaConfiguration = new Mock<IOptions<ConfigurationEntry>>();
            utilityDeltaConfiguration.Setup(x => x.Value).Returns(new ConfigurationEntry()
            {
                FILE_HANDLE_OPEN_LIMIT = FILE_HANDLE_OPEN_LIMIT,
                SUB_DIR_CONTAINERS = folder
            });
            var service = new FileHandlesManager(utilityDeltaConfiguration.Object);

            var containerNames = new string[] { "test1", "test2", "test3" };

            var test1 = service.OpenWrite(containerNames[0]);
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(1, service.NumberOfOpenStreams);

            test1.Dispose();
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(1, service.NumberOfOpenStreams);

            //Check that is is re-used
            test1 = service.OpenWrite(containerNames[0]);
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(1, service.NumberOfOpenStreams);

            test1.Dispose();
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(1, service.NumberOfOpenStreams);

            var test2 = service.OpenWrite(containerNames[1]);
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(1, service.NumberOfConnectionsToContainer(containerNames[1]));
            if (FILE_HANDLE_OPEN_LIMIT > 1)
            {
                Assert.AreEqual(2, service.NumberOfOpenStreams);
            } else
            {
                Assert.AreEqual(1, service.NumberOfOpenStreams);
            }

            test2.Dispose();
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer(containerNames[0]));
            Assert.AreEqual(0, service.NumberOfConnectionsToContainer(containerNames[1]));
            if (FILE_HANDLE_OPEN_LIMIT > 1)
            {
                Assert.AreEqual(2, service.NumberOfOpenStreams);
            }
            else
            {
                Assert.AreEqual(1, service.NumberOfOpenStreams);
            }
        }
    }
}
