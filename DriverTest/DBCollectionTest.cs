using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using SequoiaDB.Bson;
using System.Collections.Generic;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    [TestClass()]
    public class DBCollectionTest
    {
        private TestContext testContextInstance;
        private static Config config = null;

        Sequoiadb sdb = null;
        CollectionSpace cs = null;
        DBCollection coll = null;
        //string csName = "testfoo";
        //string cName = "testbar";
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
            if (config == null)
                config = new Config(); 
        }

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
                coll = cs.CreateCollection(cName);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to Initialize in DBCollectionTest, ErrorType = {0}", e.ErrorType);
                Environment.Exit(0);
            }
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            sdb.DropCollectionSpace(csName);
            sdb.Disconnect();
        }
        #endregion


        /// <summary>
        ///Testing for Insert
        ///</summary>
        [TestMethod()]
        public void InsertTest()
        {
            // insert
            BsonDocument insertor = new BsonDocument();
            string date = DateTime.Now.ToString();
            insertor.Add("operation", "Insert");
            insertor.Add("date", date);
            Object id = (ObjectId)coll.Insert(insertor);

            BsonDocument matcher = new BsonDocument();
            DBQuery query = new DBQuery();
            matcher.Add("date", date);
            query.Matcher = matcher;
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            BsonDocument bson = cursor.Next();
            Assert.IsNotNull(bson);
            Assert.IsTrue(id.Equals(bson["_id"].AsObjectId));
            //Assert.IsTrue(id.)
        }

        /// <summary>
        ///Testing for Insert
        ///</summary>
        [TestMethod()]
        public void InsertTest_WithId()
        {
            int testTypeSize = 8;
            for (int i = 0; i < testTypeSize; i++)
            {
                // insert
                BsonDocument insertor = new BsonDocument();
                string date = DateTime.Now.ToString();
                insertor.Add("operation", "Insert");
                insertor.Add("date", date);
                switch (i)
                { 
                    case 0:
                        insertor.Add("_id", 3.14);
                        break;
                    case 1:
                        insertor.Add("_id", "abcdefg");
                        break;
                    case 2:
                        insertor.Add("_id", new BsonDocument("id", "id"));
                        break;
                    case 3:
                        insertor.Add("_id", ObjectId.GenerateNewId());
                        break;
                    case 4:
                        insertor.Add("_id", true);
                        break;
                    case 5:
                        insertor.Add("_id", 1234);
                        break;
                    case 6:
                        insertor.Add("_id", 10000L);
                        break;
                    case 7:
                        insertor.Add("_id", new BsonTimestamp(1000000000L));
                        break;
                    default:
                        continue;
                }
                coll.Delete(null);
                BsonValue value = coll.Insert(insertor);
                Object id = null;
                BsonType type = value.BsonType;
                if (type == BsonType.Double)
                    id = value.AsDouble;
                else if (type == BsonType.String)
                    id = value.AsString;
                else if (type == BsonType.Document)
                    id = value.AsBsonDocument;
                else if (type == BsonType.Array)
                    id = value.AsBsonArray;
                else if (type == BsonType.Binary)
                    id = value.AsBsonBinaryData;
                else if (type == BsonType.Undefined)
                    id = value.AsBsonUndefined;
                else if (type == BsonType.ObjectId)
                    id = value.AsObjectId;
                else if (type == BsonType.Boolean)
                    id = value.AsBoolean;
                else if (type == BsonType.DateTime)
                    id = value.AsDateTime;
                else if (type == BsonType.Null)
                    ;
                else if (type == BsonType.RegularExpression)
                    id = value.AsRegex;
                else if (type == BsonType.JavaScript)
                    id = value.AsBsonJavaScript;
                else if (type == BsonType.Symbol)
                    id = value.AsBsonSymbol;
                else if (type == BsonType.JavaScriptWithScope)
                    id = value.AsBsonJavaScriptWithScope;
                else if (type == BsonType.Int32)
                    id = value.AsInt32;
                else if (type == BsonType.Timestamp)
                    id = value.AsBsonTimestamp;
                else if (type == BsonType.Int64)
                    id = value.AsInt64;
                else if (type == BsonType.MinKey)
                    id = value.AsBsonMinKey;
                else if (type == BsonType.MaxKey)
                    id = value.AsBsonMaxKey;

                BsonDocument matcher = new BsonDocument();
                DBQuery query = new DBQuery();
                matcher.Add("date", date);
                query.Matcher = matcher;
                DBCursor cursor = coll.Query(query);
                Assert.IsNotNull(cursor);
                BsonDocument bson = cursor.Next();
                Assert.IsNotNull(bson);

                BsonValue ret = bson.GetValue("_id");
                type = ret.BsonType;
                if (type == BsonType.Double)
                    Assert.IsTrue(id.Equals(ret.AsDouble));
                else if (type == BsonType.String)
                    Assert.IsTrue(id.Equals(ret.AsString));
                else if (type == BsonType.Document)
                    Assert.IsTrue(id.Equals(ret.AsBsonDocument));
                else if (type == BsonType.Array)
                    Assert.IsTrue(id.Equals(ret.AsBsonArray));
                else if (type == BsonType.Binary)
                    Assert.IsTrue(id.Equals(ret.AsBsonBinaryData));
                else if (type == BsonType.Undefined)
                    Assert.IsTrue(id.Equals(ret.AsBsonUndefined));
                else if (type == BsonType.ObjectId)
                    Assert.IsTrue(id.Equals(ret.AsObjectId));
                else if (type == BsonType.Boolean)
                    Assert.IsTrue(id.Equals(ret.AsBoolean));
                else if (type == BsonType.DateTime)
                    Assert.IsTrue(id.Equals(ret.AsDateTime));
                else if (type == BsonType.Null)
                    Assert.IsTrue(id == null);
                else if (type == BsonType.RegularExpression)
                    Assert.IsTrue(id.Equals(ret.AsRegex));
                else if (type == BsonType.JavaScript)
                    Assert.IsTrue(id.Equals(ret.AsBsonJavaScript));
                else if (type == BsonType.Symbol)
                    Assert.IsTrue(id.Equals(ret.AsBsonSymbol));
                else if (type == BsonType.JavaScriptWithScope)
                    Assert.IsTrue(id.Equals(ret.AsBsonJavaScriptWithScope));
                else if (type == BsonType.Int32)
                    Assert.IsTrue(id.Equals(ret.AsInt32));
                else if (type == BsonType.Timestamp)
                    Assert.IsTrue(id.Equals(ret.AsBsonTimestamp));
                else if (type == BsonType.Int64)
                    Assert.IsTrue(id.Equals(ret.AsInt64));
                else if (type == BsonType.MinKey)
                    Assert.IsTrue(id.Equals(ret.AsBsonMinKey));
                else if (type == BsonType.MaxKey)
                    Assert.IsTrue(id.Equals(ret.AsBsonMaxKey));
            }
        }

        /// <summary>
        ///Testing for DBCollection Operation: insert, delete, update, query
        ///</summary>
        [TestMethod()]
        public void OperationTest()
        {
            // Insert
            BsonDocument insertor = new BsonDocument();
            insertor.Add("Last Name", "Lin");
            insertor.Add("First Name", "Hetiu");
            insertor.Add("Address", "SYSU");
            BsonDocument sInsertor = new BsonDocument();
            sInsertor.Add("Phone", "10086");
            sInsertor.Add("EMail", "hetiu@yahoo.com.cn");
            insertor.Add("Contact", sInsertor);
            ObjectId insertID = (ObjectId)coll.Insert(insertor);
            Assert.IsNotNull(insertID);

            // Update
            DBQuery query = new DBQuery();
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            updater.Add("Age", 25);
            modifier.Add("$set", updater);
            matcher.Add("First Name", "Hetiu");
            query.Matcher = matcher;
            query.Modifier = modifier;
            coll.Update(query);

            // Query
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            BsonDocument bson = cursor.Next();
            Assert.IsNotNull(bson);
            Assert.IsTrue(bson["First Name"].AsString.Equals("Hetiu"));
            Assert.IsTrue(bson["Age"].AsInt32.Equals(25));

            // Delete
            BsonDocument drop = new BsonDocument();
            drop.Add("Last Name", "Lin");
            coll.Delete(drop);
            query.Matcher = drop;
            cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            bson = cursor.Next();
            Assert.IsNull(bson);
        }

        /// <summary>
        ///Testing for Query
        ///</summary>
        [TestMethod()]
        public void QueryTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "Query");
                insertor.Add("date", date);
                coll.Insert(insertor);
            }
            BsonDocument matcher = new BsonDocument();
            DBQuery query = new DBQuery();
            matcher.Add("operation", "Query");
            query.Matcher = matcher;
            query.ReturnRowsCount = 5;
            query.SkipRowsCount = 5;
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 5);
        }

        [TestMethod()]
        public void QueryOneTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "Query");
                insertor.Add("date", date);
                coll.Insert(insertor);
            }
            BsonDocument matcher = new BsonDocument();
            DBQuery query = new DBQuery();
            matcher.Add("operation", "Query");
            query.Matcher = matcher;
            query.ReturnRowsCount = 5;
            query.SkipRowsCount = 5;
            query.Flag = DBQuery.FLG_QUERY_WITH_RETURNDATA;
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            sdb.CloseAllCursors();
            int count = 0;
            while (cursor.Next() != null)
            {
                ++count;
                BsonDocument bson = cursor.Current();
                Assert.IsNotNull(bson);
            }
            Assert.IsTrue(count == 5);
        }

        /// <summary>
        ///Testing for Indexes: create index, get indexes, drop index
        ///</summary>
        [TestMethod()]
        public void IndexesTest()
        {
            // Insert
            BsonDocument insertor = new BsonDocument();
            insertor.Add("Last Name", "Lin");
            insertor.Add("First Name", "Hetiu");
            insertor.Add("Address", "SYSU");
            BsonDocument sInsertor = new BsonDocument();
            sInsertor.Add("Phone", "10086");
            sInsertor.Add("EMail", "hetiu@yahoo.com.cn");
            insertor.Add("Contact", sInsertor);
            ObjectId insertID = (ObjectId)coll.Insert(insertor);
            Assert.IsNotNull(insertID);

            // Create Index
            BsonDocument key = new BsonDocument();
            key.Add("Last Name", 1);
            key.Add("First Name", 1);
            string name = "index name";
            coll.CreateIndex(name, key, false, false);

            // Get Indexes
            DBCursor cursor = coll.GetIndex(name);
            Assert.IsNotNull(cursor);
            BsonDocument index = cursor.Next();
            Assert.IsNotNull(index);
            Assert.IsTrue(index["IndexDef"].AsBsonDocument["name"].AsString.Equals("index name"));
            
            // Drop Index
            coll.DropIndex(name);
            cursor = coll.GetIndex(name);
            Assert.IsNotNull(cursor);
            index = cursor.Next();
            Assert.IsNull(index);
        }

        [TestMethod()]
        public void GetCountTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "GetCount");
                insertor.Add("date", date);
                coll.Insert(insertor);
            }

            BsonDocument condition = new BsonDocument();
            condition.Add("operation", "GetCount");
            long count = coll.GetCount(condition);
            Assert.IsTrue(count == 10);
        }

        [TestMethod()]
        public void BulkInsertTest()
        {
            List<BsonDocument> insertor = new List<BsonDocument>();
            for (int i = 0; i < 10; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj.Add("operation", "BulkInsert");
                obj.Add("date", DateTime.Now.ToString());
                insertor.Add(obj);
            }
            coll.BulkInsert(insertor, 0);
            BsonDocument condition = new BsonDocument();
            condition.Add("operation", "BulkInsert");
            long count = coll.GetCount(condition);
            Assert.IsTrue(count == 10);
        }

        [TestMethod()]
        public void UpdateInEmptyCLTest()
        {
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            BsonDocument hint = new BsonDocument();
            BsonDocument tempMatcher = new BsonDocument();
            DBQuery query = new DBQuery();

            //条件。
            matcher.Add("operation", new BsonDocument("$et", "Update"));
            query.Matcher = matcher;

            long count = coll.GetCount(matcher);
            Assert.IsTrue(count == 0);

            // 修改。
            updater.Add("operation", "Update");
            updater.Add("type", "Insert");
            modifier.Add("$set", updater);

            tempMatcher.Add("type", new BsonDocument("$et", "Insert"));
            coll.Update(matcher, modifier, hint);
            count = coll.GetCount(tempMatcher);
            Assert.IsTrue(count == 0);
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            BsonDocument rtn = cursor.Next();
            Assert.IsNull(rtn);
        }

        [TestMethod()]
        public void UpsertTest()
        {
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            BsonDocument hint = new BsonDocument();
            BsonDocument tempMatcher = new BsonDocument();
            DBQuery query = new DBQuery();
            matcher.Add("operation", new BsonDocument("$et", "Upsert"));
            query.Matcher = matcher;

            long count = coll.GetCount(matcher);
            Assert.IsTrue(count == 0);

            // update but insert
            updater.Add("operation", "Upsert");
            updater.Add("type", "Insert");
            modifier.Add("$set", updater);
            tempMatcher.Add("type",new BsonDocument("$et","Insert"));
            coll.Upsert(matcher, modifier, hint);
            //coll.Upsert(matcher, null, null);
            count = coll.GetCount(tempMatcher);
            Assert.IsTrue(count == 1);
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            BsonDocument rtn = cursor.Next();
            Assert.IsNotNull(rtn);
            Assert.IsTrue(rtn["type"].AsString.Equals("Insert"));

            // update
            updater = new BsonDocument();
            updater.Add("type", "Update");
            modifier = new BsonDocument();
            modifier.Add("$set", updater);
            coll.Upsert(matcher, modifier, hint);

            cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            count = coll.GetCount(matcher);
            Assert.IsTrue(count == 1);
            rtn = cursor.Next();
            Assert.IsTrue(rtn["type"].AsString.Equals("Update"));
        }

        [TestMethod()]
        public void InsertChineseTest()
        {
            BsonDocument insertor = new BsonDocument();
            insertor.Add("姓名", "林海涛");
            insertor.Add("年龄", 24);
            insertor.Add("id", 2001);

            // an embedded bson object
            BsonDocument phone = new BsonDocument
                {
                    {"0", "10086"},
                    {"1", "10000"}
                };

            insertor.Add("电话", phone);

            ObjectId id = (ObjectId)coll.Insert(insertor);

            BsonDocument matcher = new BsonDocument();
            DBQuery query = new DBQuery();
            matcher.Add("姓名", "林海涛");
            query.Matcher = matcher;
            DBCursor cursor = coll.Query(query);
            Assert.IsNotNull(cursor);
            BsonDocument rtn = cursor.Next();
            Assert.IsNotNull(rtn);
            Assert.IsTrue(id.Equals(rtn["_id"].AsObjectId));
        }
/*
        [TestMethod()]
        public void RenameTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "Rename");
                insertor.Add("date", date);
                coll.Insert(insertor);
            }

            coll.Rename("RenameCL");
            BsonDocument condition = new BsonDocument();
            condition.Add("operation", "Rename");
            long count = coll.GetCount(condition);
            Assert.IsTrue(count == 10);    
        }
*/

/*
        [TestMethod()]
        public void SplitTest()
        {
            //string srcGroup = config.conf.Sets[0].SetName;
            //string destGroup = config.conf.Sets[1].SetName;
            string oRGName = "group1";
            string nRGName = config.conf.Groups[0].GroupName;
            string node1 = "ubuntu-dev1:51000";
            string node2 = config.conf.Groups[0].Nodes[0].HostName + ":"
                           + config.conf.Groups[0].Nodes[0].Port.ToString();
            //string node2 = config.conf.Sets[1].Nodes[0].HostName + ":"
            //               + config.conf.Sets[1].Nodes[0].Port.ToString();
            string hostName = config.conf.Groups[0].Nodes[0].HostName;
            int port = config.conf.Groups[0].Nodes[0].Port;
            string csName = "SplitCS";
            string cName = "SplitCL";
            string cFullName = csName+"."+cName;

            // test wether we are in cluster environment or not
            try
            { 
                sdb.GetList(SDBConst.SDB_LIST_GROUPS);
            }
            catch(Exception ex)
            {
                ex.ToString();
                return;
            }
            // create rg
            ReplicaGroup rg = sdb.GetReplicaGroup(nRGName);
            if (rg == null)
                rg = sdb.CreateReplicaGroup(nRGName);
            // create node
            ReplicaNode node = rg.GetNode(hostName, port);
            if (node == null)
            {
                string dbpath = config.conf.Groups[0].Nodes[0].DBPath;
                Dictionary<string, string> map = new Dictionary<string, string>();
                map.Add("diaglevel", config.conf.Groups[0].Nodes[0].DiagLevel);
                node = rg.CreateNode(hostName, port, dbpath, map);
            }
            rg.Start();
            // create cs
            CollectionSpace cs1 = sdb.GetCollecitonSpace(csName);
            if (cs1 == null)
                cs1 = sdb.CreateCollectionSpace(csName);
            // create cl
            DBCollection coll1 = cs1.GetCollection(cName);
            if (coll1 == null)
            {
                BsonDocument shardingKey = new BsonDocument();
                shardingKey.Add("operation", 1);
                BsonDocument options = new BsonDocument();
                options.Add("ShardingKey", shardingKey);
                string s = options.ToString();
                coll1 = cs1.CreateCollection(cName, options);
                ;
            }
            // prepare record
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "Split1");
                insertor.Add("date", date);
                coll1.Insert(insertor);
            }
            for (int i = 0; i < 10; ++i)
            {
                string date = DateTime.Now.ToString();
                BsonDocument insertor = new BsonDocument();
                insertor.Add("operation", "Split2");
                insertor.Add("date", date);
                coll1.Insert(insertor);
            }
            // get src and dest group
            string srcGroup = null;
            string destGroup = null;
            SequoiaDB cdb = new SequoiaClient(config.conf.Catalog.Address);
            cdb.Connect(config.conf.UserName, config.conf.Password);
            CollectionSpace scs = cdb.GetCollecitonSpace("SYSCAT");
            DBCollection scl = scs.GetCollection("SYSCOLLECTIONS");
            //BsonDocument query = new BsonDocument{ {"Name": cFullName} };
            //BsonDocument selector = new BsonDocument{ {"":"CataInfo"} };
            BsonDocument query = new BsonDocument();
            query.Add("Name", cFullName);
            BsonDocument selector = new BsonDocument();
            selector.Add("", "CataInfo");
            DBCursor cursor = scl.Query(query, null, null, null);
            BsonDocument obj = cursor.Next();
            BsonArray array = obj["CataInfo"].AsBsonArray;
            for( int i=0; i<array.Count; i++)
            {
                BsonDocument subObj = array[i].AsBsonDocument;
                srcGroup = subObj["GroupName"].AsString;
                if(srcGroup != null)
                    break;
            }
            if (srcGroup == null)
            {
                System.Console.WriteLine("Can't find the source group.");
                Assert.IsTrue(0 == 1);
            }
            if(srcGroup.Equals(nRGName))
               destGroup = oRGName;
            else
               destGroup = nRGName;
            // build up split condition
            BsonDocument condition = new BsonDocument();
            condition.Add("operation", "Split2");
            // split
            coll1.Split(srcGroup, destGroup, condition, null);
            // check up
            SequoiaClient sdb2 = null;
            CollectionSpace cs2 = null;
            DBCollection coll2 = null;
            if(destGroup == oRGName)
            {
                sdb2 = sdb.GetReplicaGroup(destGroup).GetNode(node1).Connect(config.conf.UserName, config.conf.Password);
                cs2 = sdb2.GetCollecitonSpace(csName);
                // sleep 1s
                System.Threading.Thread.Sleep(3000);
                coll2 = cs2.GetCollection(cName);
                long num = coll2.GetCount(condition);
                Assert.IsTrue(num == 10);
            }
            else if (destGroup == nRGName)
            {
                sdb2 = sdb.GetReplicaGroup(destGroup).GetNode(node2).Connect(config.conf.UserName, config.conf.Password);
                cs2 = sdb2.GetCollecitonSpace(csName);
                // sleep 1s
                System.Threading.Thread.Sleep(3000);
                coll2 = cs2.GetCollection(cName);
                long num = coll2.GetCount(condition);
                Assert.IsTrue( num == 10);
            }
            else
            {
                System.Console.WriteLine("Something wrong.");
                Assert.IsTrue( 0 == 1);
            }

            sdb.DropCollectionSpace(csName);
            sdb.RemoveReplicaGroup(nRGName);
            sdb2.Disconnect();
        }
 */
        [TestMethod()]
        public void AggregateTest()
        {
		    String[] command = new String[2];
		    command[0] = "{$match:{status:\"A\"}}";
		    command[1] = "{$group:{_id:\"$cust_id\",total:{$sum:\"$amount\"}}}";
		    String[] record = new String[4];
		    record[0] = "{cust_id:\"A123\",amount:500,status:\"A\"}";
	        record[1] = "{cust_id:\"A123\",amount:250,status:\"A\"}";
	        record[2] = "{cust_id:\"B212\",amount:200,status:\"A\"}";
	        record[3] = "{cust_id:\"A123\",amount:300,status:\"D\"}";
	        // insert record into database
		    for ( int i=0; i<record.Length; i++)
		    {
			    BsonDocument obj = new BsonDocument();
			    obj = BsonDocument.Parse(record[i]);
                Console.WriteLine("Record is: "+obj.ToString());
			    coll.Insert(obj);
		    }
            List<BsonDocument> list = new List<BsonDocument>();
		    for ( int i=0; i<command.Length; i++)
		    {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(command[i]);
			    list.Add(obj);
		    }

            DBCursor cursor = coll.Aggregate(list);
            int count = 0;
            while ( null != cursor.Next() )
            {
                Console.WriteLine("Result is: "+cursor.Current().ToString());
                String str = cursor.Current().ToString();
                count++ ;
            }
            Assert.IsTrue(2 == count);
        }

        [TestMethod()]
        public void GetQueryMetaTest()
        {
            try{
            // create cl
            DBCollection coll2 = null;
            string cName2 = "testbar2";
            if (cs.IsCollectionExist(cName2))
                cs.DropCollection(cName);
            coll2 = cs.CreateCollection(cName2);
            // create index
            coll2.CreateIndex("ageIndex", new BsonDocument("age", -1), false, false);
            // prepare record
            Random ro = new Random();
            int recordNum = 10000;
            List<BsonDocument> insertor = new List<BsonDocument>();
            for (int i = 0; i < recordNum; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj.Add("Id",i);
                obj.Add("age", ro.Next(0, 100));
                obj.Add("date", DateTime.Now.ToString());
                insertor.Add(obj);
            }
            coll2.BulkInsert(insertor, 0);

            // TODO:

            // query
            BsonDocument subobj = new BsonDocument();
            BsonDocument query = new BsonDocument();
            query.Add("age", subobj);
            subobj.Add("$gt", 1);
            subobj.Add("$lt", 99);
            // hint
            BsonDocument hint = new BsonDocument();
            hint.Add("", "ageIndex");
            // orderBy
            BsonDocument orderBy = new BsonDocument();
            orderBy.Add("Indexblocks", 1);
            // execute getQueryMeta
            DBCursor cursor = coll2.GetQueryMeta(query, orderBy, null, 0, -1);
            DBCursor datacursor = null ;
            long count = 0;
            while (cursor.Next() != null)
            {
                BsonDocument temp = new BsonDocument();
                temp = cursor.Current();
                BsonDocument h = new BsonDocument();
                if ( temp.Contains("Indexblocks") && temp["Indexblocks"].IsBsonArray )
                {
                     h.Add("Indexblocks", temp["Indexblocks"].AsBsonArray);
                }
                datacursor = coll2.Query(null, null, null, h, 0, -1);
                while ( datacursor.Next() != null )
                {
                    count++;
                }
            }
            Assert.IsTrue(recordNum == count);
            }catch (BaseException e)
            {
                Console.WriteLine(e.ErrorType);
                return;
            }
        }

        [TestMethod()]
        public void AttachAndDetachCollectionTest()
        {
            try
            {
                // create main cl
                DBCollection mainCL = null;
                string mainCLName = "maincl";
                if (cs.IsCollectionExist(mainCLName))
                    cs.DropCollection(mainCLName);
                BsonDocument conf = new BsonDocument
                                    {
                                        {"IsMainCL", true},
                                        {"ShardingKey", new BsonDocument
                                                        {
                                                            {"id", 1}
                                                        } 
                                        },
                                        {"ReplSize", 0}
                                    };
                mainCL = cs.CreateCollection(mainCLName, conf);
                // create sub cl
                DBCollection subCL = null;
                string subCLName = "subcl";
                if (cs.IsCollectionExist(subCLName))
                    cs.DropCollection(subCLName);
                BsonDocument conf1 = new BsonDocument
                                    {
                                        {"ReplSize", 0}
                                    };
                subCL = cs.CreateCollection(subCLName, conf1);
                // case 1: test attachCollection
                // TODO:
                int num = 10;
                // mainCL attach subCL
                BsonDocument options = new BsonDocument
                {
                    {"LowBound", new BsonDocument{{"id", 0}}},
                    {"UpBound", new BsonDocument{{"id", num}}}
                };
                string subCLFullName = cs.Name+"."+ subCLName;
                mainCL.AttachCollection(subCLFullName, options);
                // insert some records
                List<BsonDocument> insertor = new List<BsonDocument>();
                for (int i = 0; i < num + 10; i++)
                {
                    BsonDocument obj = new BsonDocument();
                    obj.Add("id", i);
                    //insertor.Add(obj);
                    try
                    {
                        mainCL.Insert(obj);
                    }
                    catch (BaseException e)
                    {
                        int errno = e.ErrorCode;
                        Console.WriteLine(e.ErrorType);
                    }
                }
                try
                {
                    //mainCL.BulkInsert(insertor, 0);
                }
                catch (BaseException e)
                {
                    Assert.IsTrue(e.ErrorType.Equals("SDB_CAT_NO_MATCH_CATALOG"));
                }
                // check
                BsonDocument subobj = new BsonDocument();
                BsonDocument matcher = new BsonDocument();
                matcher.Add("id", subobj);
                subobj.Add("$gte", 0);
                subobj.Add("$lt", num);
                DBCursor cursor = mainCL.Query(matcher, null, null, null, 0, -1);
                long count = 0;
                while (cursor.Next() != null)
                {
                    count++;
                }
                Assert.IsTrue(num == count);
                BsonDocument subobj1 = new BsonDocument();
                BsonDocument matcher1 = new BsonDocument();
                matcher1.Add("id", subobj1);
                subobj1.Add("$gte", num);
                DBCursor cursor1 = mainCL.Query(matcher1, null, null, null, 0, -1);
                count = 0;
                while (cursor1.Next() != null)
                {
                    count++;
                }
                Assert.IsTrue(0 == count);
                // case 2: test detachCollection
                // TODO:
                mainCL.DetachCollection(subCLFullName);
                // check
                try
                {
                    BsonDocument obj = new BsonDocument("test", "test");
                    mainCL.Insert(obj);
                }
                catch (BaseException e)
                {
                    Assert.IsTrue(e.ErrorType.Equals("SDB_CAT_NO_MATCH_CATALOG"));
                    Console.WriteLine(e.ErrorType);
                }
            }
            catch (BaseException e)
            {
                Console.WriteLine(e.ErrorType);
                return;
            }
        }

        [TestMethod()]
        public void AlterCollectionTest()
        {
            DBCollection coll = null;
            BsonDocument options = null;
            string clName_alter = "alterCLTest";
            string fullName = null;
            int partition = 0;
            string type = null;
            int rs = -1;
            coll = cs.CreateCollection(clName_alter);
            fullName = coll.CollSpace.Name + "." + clName_alter;
            // alter collecton attrubute
            options = new BsonDocument {
                {"ReplSize", 0},
                {"ShardingKey", new BsonDocument{{"a",1}}},
                {"ShardingType", "hash"},
                {"Partition", 4096}
            };
            coll.Alter(options);
            // check
            BsonDocument matcher = new BsonDocument { {"Name", fullName} };
            DBCursor cur = sdb.GetSnapshot(8, matcher, null, null );
            BsonDocument obj = cur.Next();
            if (null == obj)
            {
                Assert.Fail();
            }
            try
            {
                rs = obj["ReplSize"].AsInt32;
                Assert.IsTrue(7 == rs);
                partition = obj["Partition"].AsInt32;
                Assert.IsTrue(4096 == partition);
                type = obj["ShardingType"].AsString;
                Assert.IsTrue(type.Equals("hash"));
            }
            catch(System.Exception)
            {
                Assert.Fail();
            }
        }

        [TestMethod()]
        public void QueryExplainAPITest()
        {
            int num = 100;
            int i = 0;
            string indexName = "QueryExpalinIndex";
            BsonDocument index = new BsonDocument
            {
                {"age", 1}
            };
            // create index
            coll.CreateIndex(indexName, index, false, false);
            // insert records
            for (; i < num; i++)
            {
                BsonDocument obj = new BsonDocument{
                    {"firstName", "John"},
                    {"lastName", "Smith"},
                    {"age", i}
                };
                coll.Insert(obj);
            }
            // query explain
            BsonDocument cond = new BsonDocument { { "age", new BsonDocument { { "$gt", 50 } } } };
            BsonDocument sel = new BsonDocument { { "age", "" } };
            BsonDocument orderBy = new BsonDocument { { "age", -1 } };
            BsonDocument hint = new BsonDocument { { "", indexName } };
            BsonDocument options = new BsonDocument { { "Run", true } };
            DBCursor cur = coll.Explain(cond, sel, orderBy, hint, 47, 3, 0, options);
            Assert.IsNotNull(cur);
            BsonDocument record = cur.Next();
            int indexRead = record["IndexRead"].AsInt32;
            Assert.IsTrue(50 == indexRead);
            int dataRead = record["DataRead"].AsInt32;
            Assert.IsTrue(49 == dataRead);
        }

        [TestMethod()]
        public void QueryExplainTest()
        {
            int num = 100;
            int i = 0;
            string indexName = "QueryExpalinIndex";
            BsonDocument index = new BsonDocument
            {
                {"age", 1}
            };
            // create index
            coll.CreateIndex(indexName, index, false, false);
            // insert records
            for (; i < num; i++)
            {
                BsonDocument obj = new BsonDocument{
                    {"firstName", "John"},
                    {"lastName", "Smith"},
                    {"age", i}
                };
                coll.Insert(obj);
            }
            // query explain
            BsonDocument cond = new BsonDocument { { "age", new BsonDocument { { "$gt", 50 } } } };
            BsonDocument sel = new BsonDocument { { "age", "" } };
            BsonDocument orderBy = new BsonDocument { { "age", -1 } };
            BsonDocument hint = new BsonDocument { { "", indexName } };
            BsonDocument options = new BsonDocument { { "Run", true } };
            DBCursor cur = coll.Explain(cond, sel, orderBy, hint, 47, 3, 0, options);
            Assert.IsNotNull(cur);
            BsonDocument record = cur.Next();
            int indexRead = record["IndexRead"].AsInt32;
            Assert.IsTrue(50 == indexRead);
            int dataRead = record["DataRead"].AsInt32;
            Assert.IsTrue(49 == dataRead);
            int returnNum = record["ReturnNum"].AsInt32;
            // it seems not the result we expect.
            Assert.IsTrue(2 == returnNum);
        }

    }
}
