using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using SequoiaDB.Bson;
using System.Text;
using System.Collections;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    [TestClass()]
    public class SequoiadbTest
    {
        private TestContext testContextInstance;
        private static Config config = null;
        Sequoiadb sdb = null;
        CollectionSpace cs = null;
        DBCollection coll = null;
        string csName = "testfoo";
        string cName = "testbar";

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
        [ClassInitialize()]
        public static void SequoiadbInitialize(TestContext testContext)
        {
            if ( config == null )
                config = new Config();
        }
        [TestInitialize()]
        public void MyTestInitialize()
        {
            sdb = new Sequoiadb(config.conf.Coord.Address);
            sdb.Connect(config.conf.UserName, config.conf.Password);
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            cs = sdb.CreateCollectionSpace(csName);
            coll = cs.CreateCollection(cName);
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }

        #endregion
        
        [TestMethod()]
        public void ConnectTest()
        {
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
            System.Console.WriteLine(config.conf.Coord.Address.ToString());
            // check whether it is in the cluster environment or not
            if (!Constants.isClusterEnv(sdb))
            {
                System.Console.WriteLine("ConnectWithAuth is for cluster environment only.");
                return;
            }
            sdb.CreateUser("testusr", "testpwd");
            sdb2.Connect("testusr", "testpwd");
            Assert.IsNotNull(sdb.Connection);
            sdb2.RemoveUser("testusr", "testpwd");
            sdb2.Disconnect();
            Assert.IsNull(sdb2.Connection);
            try
            {
                sdb2.Connect("testusr", "testpwd");
            }
            catch (BaseException e)
            {
                Assert.IsTrue(e.ErrorType == "SDB_AUTH_AUTHORITY_FORBIDDEN");
            }
        }

        [TestMethod()]
        public void Connect_With_Serval_Arg_Test()
        {
		    List<string> list = new List<string>();
            
            list.Add("192.168.20.35:12340");
            list.Add("192.168.20.36:12340");
            list.Add("123:123");
            list.Add("");
            list.Add("192.168.20.40");
            list.Add("192.168.30.161:11810");
            list.Add("localhost:50000");
            list.Add("192.168.20.42:11810");
            list.Add("192.168.20.165:11810");
            list.Add("localhost:12340");
            list.Add("192.168.20.40:12340");

	        ConfigOptions options = new ConfigOptions();
	        options.MaxAutoConnectRetryTime = 0;
	        options.ConnectTimeout = 100;
	        // connect
	        Sequoiadb sdb1 = new Sequoiadb(list);
            sdb1.Connect("", "", options);
	        // set option and change the connect
	        options.ConnectTimeout = 2000;
	        sdb1.ChangeConnectionOptions(options);
	        // check
	        DBCursor cursor = sdb1.GetList(4, null, null, null);
	        Assert.IsTrue(cursor != null);
	        sdb1.Disconnect();
        }

        [TestMethod()]
        public void IsValidTest()
        {
            bool result = false;
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
            System.Console.WriteLine(config.conf.Coord.Address.ToString());
            sdb2.Connect("", "");
            Assert.IsNotNull(sdb2.Connection);
            // check before disconnect
            result = sdb2.IsValid();
            Assert.IsTrue(result);
            // check after disconnect
            sdb2.Disconnect();
            result = sdb2.IsValid();
            Assert.IsFalse(result);
            /*
            // check after shutdown database manually
            sdb2 = new SequoiaClient(config.conf.Coord.Address);
            sdb2.Connect("", "");
            result = true;
            result = sdb2.IsValid();
            Assert.IsFalse(result);
             */
        }

        [TestMethod()]
        [Ignore]
        public void IsClosedTest()
        {
            //bool result = false;
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
            System.Console.WriteLine(config.conf.Coord.Address.ToString());
            sdb2.Connect("", "");
            Assert.IsNotNull(sdb2.Connection);
            // TODO:
            //result = sdb2.IsClosed();
            Assert.IsFalse(false);
            // check
            sdb2.Disconnect();
            //result = sdb2.IsClosed();
            Assert.IsTrue(true);
        }

        [TestMethod()]
        public void CollectionSpaceTest()
        {
            string csName = "Test";
            string csName2 = "Test2";
            BsonDocument option = new BsonDocument();
            option.Add(SequoiadbConstants.FIELD_PAGESIZE, 4096);

            if (!sdb.IsCollectionSpaceExist(csName))
            {
                // default and with page size
                sdb.CreateCollectionSpace(csName);
            }
            Assert.IsTrue(sdb.IsCollectionSpaceExist(csName));
            sdb.DropCollectionSpace(csName);
            Assert.IsFalse(sdb.IsCollectionSpaceExist(csName));
            // with options
            sdb.CreateCollectionSpace(csName2, option);
            Assert.IsTrue(sdb.IsCollectionSpaceExist(csName2));
            sdb.DropCollectionSpace(csName2);
            Assert.IsFalse(sdb.IsCollectionSpaceExist(csName2));
        }

        [TestMethod()]
        public void ExecTest()
        {
            // insert English
            string sqlInsert = "INSERT INTO " + csName + "." + cName +
                " ( c, d, e, f ) values( 6.1, \"8.1\", \"aaa\", \"bbb\")";
            try
            {
                sdb.ExecUpdate(sqlInsert);
            }
            catch (BaseException e)
            {
                string errInfo = e.Message;
                Console.WriteLine("The error info is: " + errInfo);
                Assert.IsFalse(1 == 1); 
            }
            // insert Chinese
            string sqlInsert1 = "INSERT INTO " + csName + "." + cName +
                " ( 城市1, 城市2 ) values( \"广州\",\"上海\")";
            //string str = "INSERT into testfoo.testbar ( 城市1, 城市2 )  values( \"广州\",\"上海\" )";
            try
            {
                sdb.ExecUpdate(sqlInsert1);
            }
            catch (BaseException e)
            {
                string errInfo = e.Message;
                Console.WriteLine("The error info is: " + errInfo);
                Assert.IsFalse(1 == 1);
            }
            // select some 
            string sqlSelect = "SELECT 城市1 FROM " + csName + "." + cName;
            DBCursor cursor = sdb.Exec(sqlSelect);
            Assert.IsNotNull(cursor);
            while (cursor.Next() != null)
            {
                BsonDocument bson = cursor.Current();
                string temp = bson.ToString();
                Assert.IsNotNull(bson);
            }
            // select all
            string sqlSelect1 = "SELECT FROM " + csName + "." + cName;
            cursor = sdb.Exec(sqlSelect);
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                count++;
                BsonDocument bson = cursor.Current();
                string temp = bson.ToString();
                Assert.IsNotNull(bson);
            }
            Assert.AreEqual(count, 2);
            // remove all the record
            string sqlDel = "DELETE FROM " + csName + "." + cName;
            try
            {
                sdb.ExecUpdate(sqlDel);
            }
            catch (BaseException e)
            {
                string errInfo = e.Message;
                Console.WriteLine("The error info is: " + errInfo);
                Assert.IsFalse(1 == 1);
            }
        }

        [TestMethod()]
        public void ExecTest2()
        {
            // insert English
            string sqlInsert = "update " + csName + "." + cName +
                " set GoodCode='{0}', Count={1} where SaleRID='{2}'";
            sqlInsert = string.Format(sqlInsert, DateTime.Now.ToLongTimeString(), DateTime.Now.Millisecond, "1234567");
            try
            {
                sdb.ExecUpdate(sqlInsert);
            }
            catch (BaseException e)
            {
                Assert.Fail();
                string errInfo = e.Message;
                Console.WriteLine("The error info is: " + errInfo);
                Assert.IsFalse(1 == 1);
            }
        }

        [TestMethod()]
        public void GetSnapshotTest()
        {
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
            sdb2.Connect();
            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_CONTEXTS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            BsonDocument bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_CONTEXTS_CURRENT, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_SESSIONS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_SESSIONS_CURRENT, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_COLLECTIONS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_COLLECTIONSPACES, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_DATABASE, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor = sdb2.GetSnapshot(SDBConst.SDB_SNAP_SYSTEM, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            if (Constants.isClusterEnv(sdb))
            {
                cursor = sdb.GetSnapshot(SDBConst.SDB_SNAP_CATALOG, dummy, dummy, dummy);
                Assert.IsNotNull(cursor);
                BsonDocument obj = cursor.Next();
                Assert.IsNotNull(obj);
            }
            sdb2.Disconnect();
        }

        [TestMethod()]
        public void GetListTest()
        {
            BsonDocument dummy = new BsonDocument();
            BsonDocument bson = null;
            DBCursor cursor = null;
            // list cs
            cursor = sdb.GetList(SDBConst.SDB_LIST_COLLECTIONSPACES, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            // list cl
            cursor = sdb.GetList(SDBConst.SDB_LIST_COLLECTIONS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            BsonDocument obj = new BsonDocument("test", "test");
            coll.Insert(obj);

            // list groups
            // check whether it is in the cluster environment or not
            if (Constants.isClusterEnv(sdb))
            {
                cursor = sdb.GetList(SDBConst.SDB_LIST_GROUPS, dummy, dummy, dummy);
                Assert.IsNotNull(cursor);
                bson = cursor.Next();
                Assert.IsNotNull(bson);
            }

            // list task
            cursor = sdb.GetList(SDBConst.SDB_LIST_TASKS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);

            // list domains
            if (Constants.isClusterEnv(sdb))
            {
                string dmName = "testListDomain";
                Domain dm = sdb.CreateDomain(dmName, null);
                cursor = null;
                cursor = sdb.ListDomains(null, null, null, null);
                Assert.IsNotNull(cursor);
                Assert.IsNotNull(cursor.Next());
                sdb.DropDomain(dmName);
            }

            // list stored procedure
            cursor = sdb.GetList(SDBConst.SDB_LIST_STOREPROCEDURES, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);

            // list all the contexts
            if (Constants.isClusterEnv(sdb))
            {
                sdb.Disconnect();
                sdb = new Sequoiadb(config.conf.Data.Address);
                sdb.Connect(config.conf.UserName, config.conf.Password);
            }
            cursor = sdb.GetList(SDBConst.SDB_LIST_CONTEXTS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            // list current context
            cursor = sdb.GetList(SDBConst.SDB_LIST_CONTEXTS_CURRENT, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            // list all the sessions 
            cursor = sdb.GetList(SDBConst.SDB_LIST_SESSIONS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            // list current session
            cursor = sdb.GetList(SDBConst.SDB_LIST_SESSIONS_CURRENT, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

            // list storge units
            cursor = sdb.GetList(SDBConst.SDB_LIST_STORAGEUNITS, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNotNull(bson);

        }

        [TestMethod()]
        [Ignore]
        public void CreateReplicaCataSetTest()
        {
            try
            {
                System.Console.WriteLine(config.conf.Groups[2].Nodes[0].HostName.ToString());
                System.Console.WriteLine(config.conf.Groups[2].Nodes[0].Port.ToString());
                System.Console.WriteLine(config.conf.Groups[2].Nodes[0].DBPath.ToString());
                string str1 = config.conf.Groups[2].Nodes[0].HostName.ToString();
                string str2 = config.conf.Groups[2].Nodes[0].Port.ToString();
                string str3 = config.conf.Groups[2].Nodes[0].DBPath.ToString();
                sdb.CreateReplicaCataGroup(config.conf.Groups[2].Nodes[0].HostName,
                                            config.conf.Groups[2].Nodes[0].Port,
                                            config.conf.Groups[2].Nodes[0].DBPath,
                                            null);
            }
            catch (BaseException)
            {
            }
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Groups[2].Nodes[0].HostName,
                                        config.conf.Groups[2].Nodes[0].Port);
            sdb2.Connect();
            Assert.IsNotNull(sdb2.Connection);
            sdb2.Disconnect();
        }

        [TestMethod()]
        public void Transaction_Begin_Commit_Insert_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // transction begin
            sdb.TransactionBegin();
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // commit
            sdb.TransactionCommit();
            // check up
            DBCursor cursor = cl.Query();
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 2);
            //sdb.TransactionRollback();
        }

        [TestMethod()]
        public void Transaction_Begin_Rollback_Insert_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // transction begin
            sdb.TransactionBegin();
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // rollback
            sdb.TransactionRollback(); 
            // check up
            DBCursor cursor = cl.Query();
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 0);
        }

        [TestMethod()]
        public void Transaction_Begin_Commit_update_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // transction begin
            sdb.TransactionBegin();
            // update
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            matcher.Add("name","sam");
            modifier.Add("$set",new BsonDocument("age", 50));
            cl.Update(matcher, modifier, null);
            // commit
            sdb.TransactionCommit();
            // check up
            BsonDocument matcher1 = new BsonDocument("age", 27);
            DBCursor cursor = cl.Query(matcher1, null, null, null);
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 0);
            // check up
            cursor = cl.Query();
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 2);
        }

        [TestMethod()]
        public void Transaction_Begin_Rollback_update_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // transction begin
            sdb.TransactionBegin();
            // update
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            matcher.Add("name", "sam");
            modifier.Add("$set", new BsonDocument("age", 50));
            cl.Update(matcher, modifier, null);
            // rollback
            sdb.TransactionRollback();
            // check up
            BsonDocument matcher1 = new BsonDocument("age", 27);
            DBCursor cursor = cl.Query(matcher1, null, null, null);
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 1);
            // check up
            cursor = cl.Query();
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 2);
        }

        [TestMethod()]
        public void Transaction_Begin_Rollback_delete_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // transction begin
            sdb.TransactionBegin();
            // delete
            BsonDocument matcher = new BsonDocument();
            matcher.Add("name", "sam");
            cl.Delete(matcher);
            // check up
            DBCursor cursor = cl.Query();
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 1);
            // rollback
            sdb.TransactionRollback();
            // check up
            cursor = cl.Query(matcher, null, null, null);
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 1);
            // check up
            cursor = cl.Query();
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 2);
        }

        [TestMethod()]
        public void Transaction_Begin_Commit_delete_Test()
        {
            // create cs, cl
            string csName = "testfoo";
            string cName = "testbar";
            if (sdb.IsCollectionSpaceExist(csName))
                sdb.DropCollectionSpace(csName);
            sdb.CreateCollectionSpace(csName);
            CollectionSpace cs = sdb.GetCollecitonSpace(csName);
            DBCollection cl = cs.CreateCollection(cName);
            // insert record
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("name", "tom");
            insertor1.Add("age", 25);
            insertor1.Add("addr", "guangzhou");
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("name", "sam");
            insertor2.Add("age", 27);
            insertor2.Add("addr", "shanghai");
            cl.Insert(insertor1);
            cl.Insert(insertor2);
            // transction begin
            sdb.TransactionBegin();
            // delete
            BsonDocument matcher = new BsonDocument();
            //matcher.Add("name", new BsonDocument("$et","sam"));
            matcher.Add("name", "sam");
            cl.Delete(matcher);
            // check up
            DBCursor cursor = cl.Query();
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 1);
            // commit
            sdb.TransactionCommit();
            // check up
            cursor = cl.Query(matcher, null, null, null);
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 0);
            // chech up
            cursor = cl.Query();
            Assert.IsNotNull(cursor);
            count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 1);
        }

        ///** 
        // * need to wait for the slave node to sync
        // * i don't know how long it will take,
        // * so, i ignore this test. But, this api works.
        // */

        [TestMethod()]
        [Ignore]
        public void setSessionAttrTest()
        {
            // create another node
            string host = "192.168.20.42";
            int port = 55555;
            string dataPath = "/opt/sequoiadb/database/data/55555";
            string groupName = "group1";
            ReplicaGroup rg = null;
            try
            {
                // get the exist group
                rg = sdb.GetReplicaGroup(groupName);
                // remove the node we going to use
                var node = rg.GetNode(host, port);
                if (node != null)
                {
                    rg.RemoveNode(host, port, new BsonDocument());
                }
                // create node
                Dictionary<string, string> opt = new Dictionary<string, string>();
                rg.CreateNode(host, port, dataPath, opt);
                rg.Start();
                // insert some records first
                int num = 10;
                List<BsonDocument> insertor = new List<BsonDocument>();
                for (int i = 0; i < num; i++)
                {
                    BsonDocument obj = new BsonDocument();
                    obj.Add("id", i);
                    insertor.Add(obj);
                }
                coll.BulkInsert(insertor, 0);
                // begin a new session
                Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
                sdb2.Connect(config.conf.UserName, config.conf.Password);
                Assert.IsNotNull(sdb2.Connection);
                // TODO:
                BsonDocument conf = new BsonDocument("PreferedInstance", "m");
                sdb2.SetSessionAttr(conf);
                // check
                // record the slave note "TotalDataRead" before query
                var sddb = new Sequoiadb(host, port);
                sddb.Connect(config.conf.UserName, config.conf.Password);
                DBCursor cur1 = sddb.GetSnapshot(6, null, null, null);
                BsonDocument status1 = cur1.Next();
                long count1 = status1.GetValue("TotalDataRead").AsInt64;
                // query
                DBCursor cursor = coll.Query(null, null, null, null, 0, -1);
                BsonDocument o = new BsonDocument();
                long count = 0;
                while ((o = cursor.Next()) != null)
                    count++;
                // record the slave note "TotalRead" after query
                DBCursor cur2 = sddb.GetSnapshot(6, null, null, null);
                BsonDocument status2 = cur2.Next();
                long count2 = status2.GetValue("TotalDataRead").AsInt64;
                //Assert.IsTrue(num == count2 - count1);
                long temp = count2 - count1;
                Console.WriteLine("count2 is " + count2 + ", count1 is " + count1);
                DBCursor cur3 = sddb.GetSnapshot(6, null, null, null);
                BsonDocument status3 = cur3.Next();
                long count3 = status3.GetValue("TotalRead").AsInt64;
            }
            finally
            {
                // remove the newly build node
                var node = rg.GetNode(host, port);
                if (node != null)
                {
                    rg.RemoveNode(host, port, new BsonDocument());
                }
            }
        }

        [TestMethod()]
        public void setSessionAttr_Arguments_Test()
        {
            // begin a new session
            Sequoiadb sdb2 = new Sequoiadb(config.conf.Coord.Address);
            sdb2.Connect(config.conf.UserName, config.conf.Password);
            Assert.IsNotNull(sdb2.Connection);
            // TODO:
            BsonDocument conf = null;
            string[] str = { "M", "m", "S", "s", "A", "a" };
            int[] nodeNumber = { 1, 2, 3, 4, 5, 6, 7 };
            Random rnd = new Random();
            int r = rnd.Next(2);
            int n = -1;
            if (r == 0)
            {
                n = rnd.Next(6);
                conf = new BsonDocument("PreferedInstance", str[n]);
            }
            else
            {
                n = rnd.Next(7);
                conf = new BsonDocument("PreferedInstance", nodeNumber[n]);
            }
            try
            {
                sdb2.SetSessionAttr(conf);
            }
            catch (BaseException e) 
            {
                Console.WriteLine(e.ErrorType);
                Assert.Fail();
            }
        }

    }
}
