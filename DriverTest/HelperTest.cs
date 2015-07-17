using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    [TestClass()]
    public class HelperTest
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
        public void RoundToMultipleXTest()
        {
            byte[] byteArray = new byte[10]; 
            int multipler = 4; 
            byte[] newByte = Helper_Accessor.RoundToMultipleX(byteArray, multipler);
            Assert.AreEqual(12, newByte.Length);
        }

        [TestMethod()]
        [DeploymentItem("Driver.dll")]
        public void RoundToMultipleXLengthTest()
        {
            int inLength = 10;
            int multipler = 4;
            int expected = 12;
            int actual;
            actual = Helper_Accessor.RoundToMultipleXLength(inLength, multipler);
            Assert.AreEqual(expected, actual);
        }
    }
}
