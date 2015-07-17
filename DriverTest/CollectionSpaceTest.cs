using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    /// <summary>
    ///这是 CollectionSpaceTest 的测试类，旨在
    ///包含所有 CollectionSpaceTest 单元测试
    ///</summary>
    [TestClass()]
    public class CollectionSpaceTest
    {


        private TestContext testContextInstance;
        private static Config config = null;

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

        #region 附加测试特性
        // 
        //编写测试时，还可使用以下特性:
        //
        //使用 ClassInitialize 在运行类中的第一个测试前先运行代码
        [ClassInitialize()]
        public static void SequoiadbInitialize(TestContext testContext)
        {
            if ( config == null )
                config = new Config();
        }
        //
        //使用 ClassCleanup 在运行完类中的所有测试后再运行代码
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //使用 TestInitialize 在运行每个测试前先运行代码
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //使用 TestCleanup 在运行完每个测试后运行代码
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        [TestMethod()]
        public void CollectionTest()
        {
            string csName = "testCS1";
            string clName = "testCL1";
            CollectionSpace cs = null;
            Sequoiadb sdb = new Sequoiadb(config.conf.Coord.Address);
            sdb.Connect(config.conf.UserName, config.conf.Password);
            if(sdb.IsCollectionSpaceExist(csName))
                cs = sdb.GetCollecitonSpace(csName);
            else
                cs = sdb.CreateCollectionSpace(csName);
            if (!cs.IsCollectionExist(clName))
                cs.CreateCollection(clName);
            cs.DropCollection(clName);
            Assert.IsFalse(cs.IsCollectionExist(clName));
            sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }
    }
}
