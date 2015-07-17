using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using SequoiaDB.Bson;
using SequoiaDB.Driver;

namespace DriverTest
{

    [TestClass()]
    public class DBCursorTest
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
            //cs.DropCollection(cName);
            //sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }
        #endregion
    /*    
        [TestMethod()]
        public void UpdateCurrentTest()
        {
            BsonDocument insertor = new BsonDocument();
            insertor.Add("Last Name", "Lin");
            insertor.Add("First Name", "Hetiu");
            insertor.Add("Address", "SYSU");
            BsonDocument sInsertor = new BsonDocument();
            sInsertor.Add("Phone", "10086");
            sInsertor.Add("EMail", "hetiu@yahoo.com.cn");
            insertor.Add("Contact", sInsertor);
            coll.Insert(insertor);

            BsonDocument dummy = new BsonDocument();
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            updater.Add("Age", 25);
            modifier.Add("$group", updater);
            matcher.Add("First Name", "Hetiu");
            DBCursor cursor = coll.Query(matcher, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            BsonDocument bson = cursor.Next();
            Assert.IsNotNull(bson);

            cursor.UpdateCurrent(modifier);
            bson = cursor.Current();
            Assert.IsNotNull(bson);
            Assert.IsTrue(bson["First Name"].AsString.Equals("Hetiu"));
            Assert.IsTrue(bson["Age"].AsInt32.Equals(25));
        }

        [TestMethod()]
        public void DeleteCurrentTest()
        {
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("Last Name", "Lin");
            insertor1.Add("First Name", "Hetiu");
            coll.Insert(insertor1);
            BsonDocument insertor2 = new BsonDocument();
            insertor2.Add("Last Name", "Wang");
            insertor2.Add("First Name", "Tao");
            coll.Insert(insertor2);

            BsonDocument dummy = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            matcher.Add("First Name", "Hetiu");
            DBCursor cursor = coll.Query(matcher, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            BsonDocument bson = cursor.Next();
            Assert.IsNotNull(bson);
            cursor.DeleteCurrent();
            Assert.IsNull(cursor.Current());

            BsonDocument matcher2 = new BsonDocument();
            matcher2.Add("First Name", "Tao");
            DBCursor cursor2 = coll.Query(matcher2, dummy, dummy, dummy);
            Assert.IsNotNull(cursor2);
            BsonDocument bson2 = cursor2.Next();
            Assert.IsNotNull(bson2);
            Assert.IsTrue(coll.GetCount(dummy) == 1);
        }
  */
        
        [TestMethod()]
        public void CurrentTest()
        {
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("Last Name", "怪");
            insertor1.Add("First Name", "啊");
            insertor1.Add("Address", "SYSU");
            BsonDocument sInsertor1 = new BsonDocument();
            sInsertor1.Add("Phone", "10000");
            sInsertor1.Add("EMail", "10000@yahoo.com.cn");
            insertor1.Add("Contact", sInsertor1);
            coll.Insert(insertor1);

            BsonDocument dummy = new BsonDocument();
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            updater.Add("Age", 25);
            modifier.Add("$group", updater);
            matcher.Add("First Name", "啊");
            DBCursor cursor = coll.Query(matcher, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            BsonDocument obj = new BsonDocument();
            obj = cursor.Current();
            Assert.IsNotNull(obj);

            Assert.IsTrue(obj["First Name"].AsString.Equals("啊"));
            Assert.IsTrue(obj["Last Name"].AsString.Equals("怪"));
            Assert.IsTrue(obj["Address"].AsString.Equals("SYSU"));
        }
        
        [TestMethod()]
        public void NextTest()
        {
            BsonDocument insertor = new BsonDocument();
            insertor.Add("Last Name", "Lin");
            insertor.Add("First Name", "Hetiu");
            insertor.Add("Address", "SYSU");
            BsonDocument sInsertor = new BsonDocument();
            sInsertor.Add("Phone", "10086");
            sInsertor.Add("EMail", "hetiu@yahoo.com.cn");
            insertor.Add("Contact", sInsertor);
            coll.Insert(insertor);

            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("Last Name", "怪");
            insertor1.Add("First Name", "啊");
            insertor1.Add("Address", "SYSU");
            BsonDocument sInsertor1 = new BsonDocument();
            sInsertor1.Add("Phone", "10000");
            sInsertor1.Add("EMail", "10000@yahoo.com.cn");
            insertor1.Add("Contact", sInsertor);
            coll.Insert(insertor1);

            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = coll.Query(dummy, dummy, dummy, dummy);
            BsonDocument obj1 = new BsonDocument();
            BsonDocument obj2 = new BsonDocument();
            obj1 = cursor.Current();
            Assert.IsNotNull(obj1);
            obj2 = cursor.Next();
            Assert.IsNotNull(obj2);

            Assert.IsTrue(obj1["Address"].AsString.Equals("SYSU"));
            Assert.IsTrue(obj2["Address"].AsString.Equals("SYSU"));
        }

        [TestMethod()]
        public void CloseTest()
        {
            int num = 10 ;
            for( int i = 0; i < num; i++ )
            {
                BsonDocument insertor = new BsonDocument();
                insertor.Add("num", i);
                coll.Insert(insertor);
            }

            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = coll.Query(dummy, dummy, dummy, dummy);
            BsonDocument obj = new BsonDocument();
            obj = cursor.Current();
            Assert.IsNotNull(obj);
            obj = cursor.Next();
            Assert.IsNotNull(obj);
            // DO:
            cursor.Close();
            // close
            try
            {
                cursor.Close();
            }catch( BaseException )
            {
                Assert.Fail();
                //Console.WriteLine("After close cursor, call Close() get errno " +　e.ErrorCode );
                //Assert.IsTrue(e.ErrorType.Equals("SDB_DMS_CONTEXT_IS_CLOSE"));
            }
            // current
            try
            {
                cursor.Current();
            }
            catch (BaseException e)
            {
                Console.WriteLine("After close cursor, call Current() get errno " + e.ErrorCode);
                Assert.IsTrue(e.ErrorType.Equals("SDB_DMS_CONTEXT_IS_CLOSE"));
            }
            // next
            try
            {
                cursor.Next();
            }
            catch (BaseException e)
            {
                Console.WriteLine("After close cursor, call Next() get errno " + e.ErrorCode);
                Assert.IsTrue(e.ErrorType.Equals("SDB_DMS_CONTEXT_IS_CLOSE"));
            }

        }
        
        [TestMethod()]
        public void CloseAllCursorsTest()
        {
            int num = 10000;
            for (int i = 0; i < num; i++)
            {
                BsonDocument insertor = new BsonDocument();
                insertor.Add("num", i);
                coll.Insert(insertor);
            }

            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = coll.Query(dummy, dummy, dummy, dummy);
            DBCursor cursor1 = coll.Query(dummy, dummy, dummy, dummy);
            DBCursor cursor2 = coll.Query(dummy, dummy, dummy, dummy);
            BsonDocument obj = new BsonDocument();
            obj = cursor1.Next();
            obj = cursor2.Next();
            // DO:
            sdb.CloseAllCursors();
            // cursor
            try
            {
                while (null != cursor.Next()) { }
            }
            catch (BaseException e)
            {
                int eno = e.ErrorCode;
                Assert.IsTrue(e.ErrorType.Equals("SDB_RTN_CONTEXT_NOTEXIST"));
            }
            // cursor1
            try
            {
                while (null != cursor1.Next()) { }
            }
            catch (BaseException e)
            {
                int eno = e.ErrorCode;
                Assert.IsTrue(e.ErrorType.Equals("SDB_RTN_CONTEXT_NOTEXIST"));
            }
            // curosr2
            try
            {
                obj = cursor2.Current();
                cursor2.Close();
            }
            catch (BaseException)
            {
                Assert.Fail();
                //int eno = e.ErrorCode;
                //Assert.IsTrue(e.ErrorType.Equals("SDB_RTN_CONTEXT_NOTEXIST"));
            }
        }

        [TestMethod()]
        public void EmptyCursorTest()
        {
            int count = 0;
            BsonDocument insertor1 = new BsonDocument();
            insertor1.Add("Last Name", "怪");
            insertor1.Add("First Name", "啊");
            insertor1.Add("Address", "SYSU");
            coll.Insert(insertor1);

            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = coll.Query(dummy, dummy, dummy, dummy);
            Assert.IsNotNull(cursor);
            BsonDocument obj = new BsonDocument();
            while (null != (obj = cursor.Next()))
            {
                count++;
            }
            Assert.AreEqual(1, count);
        }

        [TestMethod()]
        [Ignore]
        public void CursorGetMoreTest()
        {
            int num = 10000;
            for (int i = 0; i < num; i++)
            {
                BsonDocument insertor = new BsonDocument();
                insertor.Add("num", i);
                coll.Insert(insertor);
            }

            BsonDocument dummy = new BsonDocument();
            DBCursor cursor = coll.Query(dummy, dummy, dummy, dummy);
            BsonDocument obj = new BsonDocument();
            // cursor
            try
            {
                while (null != cursor.Next()) { }
            }
            catch (BaseException e)
            {
                int eno = e.ErrorCode;
                Assert.IsTrue(e.ErrorType.Equals("SDB_RTN_CONTEXT_NOTEXIST"));
            }
        }


    }
}
