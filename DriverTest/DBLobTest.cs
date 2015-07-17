using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using SequoiaDB.Bson;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    /// <summary>
    ///这是 DBLobTest 的测试类，旨在
    ///包含所有 DBLobTest 单元测试
    ///</summary>
    [TestClass()]
    public class DBLobTest
    {

        private TestContext testContextInstance;
        private static Config config = null;

        Sequoiadb sdb = null;
        CollectionSpace cs = null;
        DBCollection cl = null;
        string csName = "lobtestfoo";
        string cName = "lobtestbar";

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
                if (sdb.IsCollectionSpaceExist(csName))
                    sdb.DropCollectionSpace(csName);
                cs = sdb.CreateCollectionSpace(csName);
                cl = cs.CreateCollection(cName);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to Initialize in DBLobTest, ErrorType = {0}", e.ErrorType);
                Environment.Exit(0);
            }
        }
        
        //使用 TestCleanup 在运行完每个测试后运行代码
        [TestCleanup()]
        public void MyTestCleanup()
        {
            sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }
        
        #endregion

        [TestMethod()]
        public void LobGlobalTest()
        {
            DBLob lob = null;
            DBLob lob2 = null;
            DBLob lob3 = null;
            bool flag = false;
            ObjectId oid1 = ObjectId.Empty;
            ObjectId oid2 = ObjectId.Empty;
            ObjectId oid3 = ObjectId.Empty;
            ObjectId oid4 = ObjectId.Empty;
            long size1 = 0;
            long time1 = 0;
            int bufSize = 1000;
            int readNum = 0;
            int retNum = 0;
            int offset = 0;
            int i = 0;
            byte[] readBuf = null;
            byte[] buf = new byte[bufSize];
            for (i = 0; i < bufSize; i++)
            {
                buf[i] = 65;
            }
            DBCursor cursor = null;
            BsonDocument record = null;
            long lobSize = 0;

            /// case 1: create a new lob
            // CreateLob
            lob = cl.CreateLob();
            Assert.IsNotNull(lob);
            // IsClosed
            flag = true;
            flag = lob.IsClosed();
            Assert.IsFalse(flag);
            // GetID
            oid1 = lob.GetID();
            Assert.IsTrue(ObjectId.Empty != oid1);
            // Write
            lob.Write(buf);
            // Close
            lob.Close();
            // IsClosed
            flag = false;
            flag = lob.IsClosed();
            Assert.IsTrue(flag);

            // case 2: open an exsiting lob
            lob2 = cl.OpenLob(oid1);
            Assert.IsNotNull(lob2);
            // IsClosed
            flag = true;
            flag = lob2.IsClosed();
            Assert.IsFalse(flag);
            // GetID
            oid2 = lob2.GetID();
            Assert.IsTrue(ObjectId.Empty != oid2);
            Assert.IsTrue(oid1 == oid2);
            // GetSize
            size1 = lob2.GetSize();
            Assert.IsTrue(bufSize == size1);
            // GetCreateTime
            time1 = lob2.GetCreateTime();
            Assert.IsTrue(time1 > 0);
            // Read
            readNum = bufSize / 4;
            readBuf = new Byte[readNum];
            retNum = lob2.Read(readBuf);
            Assert.IsTrue(readNum == retNum);
            // Seek
            offset = bufSize / 2;
            lob2.Seek(offset, DBLob.SDB_LOB_SEEK_CUR);
            // Read
            retNum = 0;
            retNum = lob2.Read(readBuf);
            Assert.IsTrue(readNum == retNum);
            // Close
            lob2.Close();
            // IsClosed
            flag = false;
            flag = lob2.IsClosed();
            Assert.IsTrue(flag);

            /// case 3: create a lob with specified oid
            oid3 = ObjectId.GenerateNewId();
            lob3 = cl.CreateLob(oid3);
            Assert.IsNotNull(lob3);
            // GetID
            oid4 = lob3.GetID();
            Assert.IsTrue(ObjectId.Empty != oid4);
            Assert.IsTrue(oid3 == oid4);
            // Write
            lob3.Write(buf);
            // Close
            lob3.Close();
            // IsClosed
            flag = false;
            flag = lob3.IsClosed();
            Assert.IsTrue(flag);

            /// case 4: test api in cl
            // ListLobs
            cursor = cl.ListLobs();
            Assert.IsNotNull(cursor);
            i = 0;
            while(null != (record = cursor.Next()))
            {
                i++;
                if (record.Contains("Size") && record["Size"].IsInt64)
                {
                    lobSize = record["Size"].AsInt64;
                }
                else
                {
                    Assert.Fail();
                }
            }
            Assert.IsTrue(2 == i);
            // RemoveLobs
            cl.RemoveLob(oid3);
            // ListLobs
            cursor = cl.ListLobs();
            Assert.IsNotNull(cursor);
            i = 0;
            while (null != (record = cursor.Next()))
            {
                i++;
                if (record.Contains("Size") && record["Size"].IsInt64)
                {
                    lobSize = record["Size"].AsInt64;
                }
                else
                {
                    Assert.Fail();
                }
            }
            Assert.IsTrue(1 == i);
        }

        [TestMethod()]
        //[Ignore]
        public void LobWriteTest()
        {
            DBLob lob = null;
            DBLob lob2 = null;
            bool flag = false;
            ObjectId oid1 = ObjectId.Empty;
            ObjectId oid2 = ObjectId.Empty;
            ObjectId oid3 = ObjectId.Empty;
            ObjectId oid4 = ObjectId.Empty;
            long size1 = 0;
            int bufSize = 1000;
            int i = 0;
            byte[] buf = new byte[bufSize];
            for (i = 0; i < bufSize; i++)
            {
                buf[i] = 65;
            }

            /// case 1: write double times
            // CreateLob
            lob = cl.CreateLob();
            Assert.IsNotNull(lob);
            // GetID
            oid1 = lob.GetID();
            Assert.IsTrue(ObjectId.Empty != oid1);
            // Write, first time
            lob.Write(buf);
            size1 = lob.GetSize();
            Assert.AreEqual(bufSize, size1);
            // Write the second time
            lob.Write(buf);
            size1 = lob.GetSize();
            Assert.AreEqual(bufSize * 2, size1);
            // Close
            lob.Close();
            // IsClosed
            flag = false;
            flag = lob.IsClosed();
            Assert.IsTrue(flag);

            /// case 2: write large date
            bufSize = 1024*1024*100;
            byte[] buf2 = new byte[bufSize];
            for (i = 0; i < bufSize; i++)
            {
                buf2[i] = 65;
            }

            // CreateLob
            lob2 = cl.CreateLob();
            Assert.IsNotNull(lob2);
            // Write, first time
            lob2.Write(buf2);
            size1 = lob2.GetSize();
            Assert.AreEqual(bufSize, size1);
            // Close
            lob2.Close();
            // IsClosed
            flag = false;
            flag = lob2.IsClosed();
            Assert.IsTrue(flag);
            // GetSize
            size1 = lob2.GetSize();
            Assert.AreEqual(bufSize, size1);
        }

        [TestMethod()]
        public void LobReadTest()
        {
            DBLob lob = null;
            DBLob lob2 = null;
            bool flag = false;
            ObjectId oid1 = ObjectId.Empty;
            int bufSize = 1024 * 1024 * 100;
            int readNum = 0;
            int retNum = 0;
            int i = 0;
            byte[] readBuf = null;
            byte[] buf = new byte[bufSize];
            for (i = 0; i < bufSize; i++)
            {
                buf[i] = 65;
            }
            long lobSize = 0;

            // CreateLob
            lob = cl.CreateLob();
            Assert.IsNotNull(lob);
            // GetID
            oid1 = lob.GetID();
            Assert.IsTrue(ObjectId.Empty != oid1);
            // Write
            lob.Write(buf);
            lobSize = lob.GetSize();
            Assert.AreEqual(bufSize, lobSize);
            // Close
            lob.Close();

            // Open lob
            lob2 = cl.OpenLob(oid1);
            lobSize = lob2.GetSize();
            Assert.AreEqual(bufSize, lobSize);
            // Read
            int skipNum = 1024*1024*50;
            lob2.Seek(skipNum, DBLob.SDB_LOB_SEEK_SET);  // after this, the offset is 1024*1024*50
            readNum = 1024*1024*10;
            readBuf = new byte[readNum];
            retNum = lob2.Read(readBuf);  // after this, the offset is 1024*1024*60
            Assert.IsTrue(readNum == retNum);
            // check
            for(i = 0; i < readBuf.Length; i++)
            {
                Assert.IsTrue(readBuf[i] == 65);
            }
            skipNum = 1024*1024*10;
            lob2.Seek(skipNum, DBLob.SDB_LOB_SEEK_CUR); // after this, the offset is 1024*1024*70
            readBuf = new byte[readNum];
            retNum = lob2.Read(readBuf);
            Assert.IsTrue(readNum == retNum); // after this, the offset is 1024*1024*80
            // check
            for(i = 0; i < readBuf.Length; i++)
            {
                Assert.IsTrue(readBuf[i] == 65);
            } 
            skipNum = 1024*1024*20;
            lob2.Seek(skipNum, DBLob.SDB_LOB_SEEK_END);
            readNum = 1024*1024*30; // will only read 1024*1024*20
            readBuf = new byte[readNum];
            retNum = lob2.Read(readBuf); // after this, the offset is 1024*1024*100
            Assert.IsTrue(readNum == (retNum + 1024*1024*10));
            
            // Close
            lob2.Close();
            // IsClosed
            flag = false;
            flag = lob.IsClosed();
            Assert.IsTrue(flag);
        }

    }
}
