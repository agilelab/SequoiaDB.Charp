using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using SequoiaDB.Bson;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    /// <summary>
    ///这是 DomainTest 的测试类，旨在
    ///包含所有 DomainTest 单元测试
    ///</summary>
    [TestClass()]
    public class DomainTest
    {
        Sequoiadb sdb = null;
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

        //使用 ClassCleanup 在运行完类中的所有测试后再运行代码
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
        }
        
        //使用 TestInitialize 在运行每个测试前先运行代码
        [TestInitialize()]
        public void MyTestInitialize()
        {
            try
            {
                sdb = new Sequoiadb(config.conf.Coord.Address);
                sdb.Connect(config.conf.UserName, config.conf.Password);
/*
                if (sdb.IsCollectionSpaceExist(csName))
                    sdb.DropCollectionSpace(csName);
                cs = sdb.CreateCollectionSpace(csName);
                coll = cs.CreateCollection(cName);
 */
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to Initialize in DomainTest, ErrorType = {0}", e.ErrorType);
                Environment.Exit(0);
            }
        }
        
        //使用 TestCleanup 在运行完每个测试后运行代码
        [TestCleanup()]
        public void MyTestCleanup()
        {
            //sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }
        
        #endregion


        [TestMethod()]
        public void DomainGlobalTest()
        {
            string dmName = "testDomainInCS";
            string csName = "domainCS";
            string clName = "domainCL";
            string groupName = config.conf.Groups[0].GroupName;
            BsonDocument options = new BsonDocument();
            BsonArray arr = new BsonArray();
            arr.Add(groupName);
            options.Add(SequoiadbConstants.FIELD_GROUPS, arr);
            Domain dm = null;
            DBCursor cur = null;
            BsonDocument record = null;

            /// IsDomainExist
            bool flag = sdb.IsDomainExist(dmName);
            Assert.IsFalse(flag);

            /// getDomain
            try
            {
               dm = sdb.GetDomain(dmName);
            }
            catch(BaseException e)
            {
                Assert.IsTrue(e.ErrorType.Equals("SDB_CAT_DOMAIN_NOT_EXIST"));
            }
            
            /// createDomain
            dm = null;
            dm = sdb.CreateDomain(dmName, options);
            Assert.IsNotNull(dm);

            /// IsDomainExist
            flag = false;
            flag = sdb.IsDomainExist(dmName);
            Assert.IsTrue(flag);

            /// getDomain
            dm = null;
            dm = sdb.GetDomain(dmName);
            Assert.IsNotNull(dm);

            /// listDomains
            cur = sdb.ListDomains(null, null, null, null);
            Assert.IsNotNull(cur);
            record = cur.Next();
            Assert.IsNotNull(record);

            /// getName
            string name = dm.Name;
            Assert.IsTrue(name.Equals(dmName));

            // create cs
            BsonDocument opts1 = new BsonDocument();
            opts1.Add("Domain", dmName);
            CollectionSpace cs = sdb.CreateCollectionSpace(csName, opts1);
            // create cl
            BsonDocument opts2 = new BsonDocument();
            //BsonDocument key = new BsonDocument();
            //key.Add("a", 1);
            opts2.Add("ShardingKey", new BsonDocument("a", 1));
            opts2.Add("ShardingType", "hash");
            opts2.Add("AutoSplit", true);
            DBCollection cl = cs.CreateCollection(clName, opts2);
            /// listCS
            cur = dm.ListCS();
            Assert.IsNotNull(cur);
            record = cur.Next();
            Assert.IsNotNull(record);

            /// listCL
            cur = dm.ListCL();
            Assert.IsNotNull(cur);
            record = cur.Next();
            Assert.IsNotNull(record);

            // dropCS
            sdb.DropCollectionSpace(csName);

            /// alter
            /// TODO: alter should be verified
            BsonDocument opts3 = new BsonDocument();
            BsonArray arr2 = new BsonArray();
            opts3.Add("Groups", arr2);
            opts3.Add("AutoSplit", false);
            dm.Alter(opts3);

            /// listDomains
            cur = sdb.ListDomains(null, null, null, null);
            Assert.IsNotNull(cur);
            record = cur.Next();
            Assert.IsNotNull(record);
            
            /// dropDomain
            sdb.DropDomain(dmName);

            /// listDomains
            cur = sdb.ListDomains(null, null, null, null);
            Assert.IsNotNull(cur);
            record = cur.Next();
            Assert.IsNull(record);
        }
    }
}
