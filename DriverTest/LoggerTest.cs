using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    [TestClass()]
    public class LoggerTest
    {


        private TestContext testContextInstance;

        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        [TestMethod()]
        [DeploymentItem("Driver.dll")]
        public void DebugTest()
        {
            Logger logger = new Logger("LoggerTest");
            string msg = "Test";
            logger.Debug(msg);
        }
    }
}
