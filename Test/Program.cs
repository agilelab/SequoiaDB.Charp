using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SequoiaDB.Driver;
using SequoiaDB.Bson;
using AgileHIS.Entities;
using EAS.Data.ORM;
using EAS.Data.Linq;
using SequoiaDB.Driver.Linq;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Sequoiadb sdb = new Sequoiadb("192.168.23.57:50000");
            sdb.Connect("", "");

            ////Test1(sdb);
            //Test2(sdb);
            ////TestHRoot(sdb);
            //TestHRoot2(sdb);

            //Test4(sdb);
            //TestLinq(sdb);
            //TestAggregate(sdb);
            //TestAggregate2(sdb);
            TestUpdate(sdb);
            //TestAggregate5(sdb);
        }

        /// <summary>
        /// Update测试。
        /// </summary>
        /// <param name="sdb"></param>
        static void TestUpdate(Sequoiadb sdb)
        {
            // The collection space name
            string csName = "sample";
            // The collection name
            string cName = "sample";

            // connect
            CollectionSpace cs;
            if (sdb.IsCollectionSpaceExist(csName))
                cs = sdb.GetCollecitonSpace(csName);
            else
                cs = sdb.CreateCollectionSpace(csName);

            DBCollection coll = null;
            if (cs.IsCollectionExist(cName))
                coll = cs.GetCollection(cName);
            else
                coll = cs.CreateCollection(cName);

            // delete all records from the collection
            BsonDocument bson = new BsonDocument();
            coll.Delete(bson);
            
            String[] record = new String[4];
            record[0] = "{cust_id:\"A123\",amount:500,status:\"A\"}";
            record[1] = "{cust_id:\"A123\",amount:250,status:\"A\"}";
            record[2] = "{cust_id:\"B212\",amount:200,status:\"A\"}";
            record[3] = "{cust_id:\"A123\",amount:300,status:\"D\"}";
            // insert record into database
            for (int i = 0; i < record.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(record[i]);
                Console.WriteLine("Record is: " + obj.ToString());
                coll.Insert(obj);
            }
            
            //准备update
            BsonDocument updater = new BsonDocument();
            BsonDocument matcher = new BsonDocument();
            BsonDocument modifier = new BsonDocument();
            BsonDocument hint = new BsonDocument();           

            //条件
            matcher.Add("cust_id", new BsonDocument("$et", "A123"));
            //更新。
            updater.Add("amount", "1000");
            updater.Add("status", "C");
            modifier.Add("$set", updater);
            //update
            coll.Update(matcher, modifier, hint);

            System.Console.ReadLine();
        }

        static void TestAggregate5(Sequoiadb sdb)
        {
            // The collection space name
            string csName = "sample";
            // The collection name
            string cName = "sample";

            // connect
            CollectionSpace cs;
            if (sdb.IsCollectionSpaceExist(csName))
                cs = sdb.GetCollecitonSpace(csName);
            else
                cs = sdb.CreateCollectionSpace(csName);

            DBCollection coll = null;
            if (cs.IsCollectionExist(cName))
                coll = cs.GetCollection(cName);
            else
                coll = cs.CreateCollection(cName);

            // delete all records from the collection
            BsonDocument bson = new BsonDocument();
            coll.Delete(bson);

            String[] command = new String[2];
            command[0] = "{$match:{status:\"A\"}}";
            command[1] = "{$group:{_id:\"$cust_id\",amount:{\"$sum\":\"$amount\"},cust_id:{\"$first\":\"$cust_id\"}}}";
            String[] record = new String[4];
            record[0] = "{cust_id:\"A123\",amount:500,status:\"A\"}";
            record[1] = "{cust_id:\"A123\",amount:250,status:\"A\"}";
            record[2] = "{cust_id:\"B212\",amount:200,status:\"A\"}";
            record[3] = "{cust_id:\"A123\",amount:300,status:\"D\"}";
            // insert record into database
            for (int i = 0; i < record.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(record[i]);
                Console.WriteLine("Record is: " + obj.ToString());
                coll.Insert(obj);
            }
            List<BsonDocument> list = new List<BsonDocument>();
            for (int i = 0; i < command.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(command[i]);
                list.Add(obj);
            }

            DBCursor cursor = coll.Aggregate(list);
            int count = 0;
            while (null != cursor.Next())
            {
                Console.WriteLine("Result is: " + cursor.Current().ToString());
                String str = cursor.Current().ToString();
                count++;
            }

            System.Console.ReadLine();
        }

        /// <summary>
        /// Linq测试。
        /// </summary>
        /// <param name="sdb"></param>
        static void TestLinq2(Sequoiadb sdb)
        {
            //求集合空间。
            var cs = sdb.GetCollecitonSpace("dbo");

            //求集合。
            var coll = cs.GetCollection<HFareDetail>();

            //执行数据插入。
            List<HFareDetail> vList =null;
            using (AgileHIS.Entities.DbEntities db = new AgileHIS.Entities.DbEntities())
            {
                vList = db.HFareDetails.ToList();
                //插入。
                foreach (var item in vList)
                {
                    coll.Insert(item);
                }
                System.Console.WriteLine(string.Format("insert {0} records", vList.Count));
                System.Console.ReadLine();
            }

            //按条件修改某一条数据的几个属性值。
            var v1 = vList.FirstOrDefault();
            v1.Name = string.Empty;
            v1.Cash = decimal.Zero;
            coll.Update(v1, p => p.ID == v1.ID);
            //按条件指量修改,指定某几个必，其他属性全部置空。
            coll.Update(p => new HFareDetail { Cash = decimal.Zero, Name = string.Empty, Price = decimal.Zero }, p => p.ChargeTime >DateTime.Now.AddDays(-1));
            //依据条件删除
            coll.Delete(p => p.ChargeTime > DateTime.Now.AddDays(-1));

            //求Count
            int count = coll.AsQueryable<HFareDetail>()
                .Where(p => p.SourceID==0)
                .Count();

            //Linq查询Take\Skip。
            var vList2 = coll.AsQueryable<HFareDetail>()
                .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
                .Skip(10).Take(1000)
                .ToList();
            System.Console.WriteLine(string.Format("query {0} records", vList.Count));

            //Linq查询过。
            var vFare = coll.AsQueryable<HFareDetail>()
                .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
                .FirstOrDefault();
            System.Console.WriteLine(vFare);

            //Linq\聚合运算，目前因为测试驱动报错，暂未实现
            var sum = coll.AsQueryable<HFareDetail>()
                .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
                .Sum(p => p.Cash);

            System.Console.ReadLine();
        }

        static void TestAggregate2(Sequoiadb sdb)
        {
            var cs = sdb.GetCollecitonSpace("dbo");

            DBCollection coll = null;
            if (cs.IsCollectionExist("t2"))
                coll = cs.GetCollection("t2");
            else
                coll = cs.CreateCollection("t2");

            String[] command = new String[2];
            command[0] = "{$match:{status:\"A\"}}";
            command[1] = "{$group:{_id:\"$cust_id\",amount:{\"$sum\":\"$amount\"},cust_id:{\"$first\":\"$cust_id\"}}}";
            String[] record = new String[4];
            record[0] = "{cust_id:\"A123\",amount:500,status:\"A\"}";
            record[1] = "{cust_id:\"A123\",amount:250,status:\"A\"}";
            record[2] = "{cust_id:\"B212\",amount:200,status:\"A\"}";
            record[3] = "{cust_id:\"A123\",amount:300,status:\"D\"}";
            // insert record into database
            for (int i = 0; i < record.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(record[i]);
                Console.WriteLine("Record is: " + obj.ToString());
                coll.Insert(obj);
            }
            List<BsonDocument> list = new List<BsonDocument>();
            for (int i = 0; i < command.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                obj = BsonDocument.Parse(command[i]);
                list.Add(obj);
            }

            DBCursor cursor = coll.Aggregate(list);
            int count = 0;
            while (null != cursor.Next())
            {
                Console.WriteLine("Result is: " + cursor.Current().ToString());
                String str = cursor.Current().ToString();
                count++;
            }
        }

        /// <summary>
        /// Linq测试。
        /// </summary>
        /// <param name="sdb"></param>
        static void TestAggregate(Sequoiadb sdb)
        {
            var cs = sdb.GetCollecitonSpace("dbo");
            var coll = cs.GetCollection<HFareDetail>();

            String[] command = new String[2];
            //command[0] = "{$match:{PNumber:\"3\"}}";
            command[0] = string.Empty;
            command[1] = "{$group:{_id:\"$EDeptID\",total:{$sum:\"$Cash\"}}}";
            List<BsonDocument> list = new List<BsonDocument>();
            for (int i = 0; i < command.Length; i++)
            {
                BsonDocument obj = new BsonDocument();
                if (!string.IsNullOrEmpty(command[i]))
                {
                    obj = BsonDocument.Parse(command[i]);
                }
                list.Add(obj);
            }

            DBCursor cursor = coll.Aggregate(list);
            int count = 0;
            while (null != cursor.Next())
            {
                Console.WriteLine("Result is: " + cursor.Current().ToString());
                String str = cursor.Current().ToString();
                count++;
            }

            System.Console.ReadLine();
        }

        /// <summary>
        /// Linq测试。
        /// </summary>
        /// <param name="sdb"></param>
        static void TestLinq(Sequoiadb sdb)
        {
            var cs = sdb.GetCollecitonSpace("dbo");
            var coll = cs.GetCollection<HFareDetail>();

            var vList = coll.AsQueryable<HFareDetail>()
                .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
                .Skip(10).Take(1000)
                .ToList();
            System.Console.WriteLine(string.Format("query {0} records", vList.Count));

            var vList2 = coll.AsQueryable<HFareDetail>()
                .Where(p => p.PNumber == 624)
                .ToList();
            System.Console.WriteLine(string.Format("query {0} records", vList2.Count));

            int count = coll.AsQueryable<HFareDetail>()
                .Where(p => p.PNumber == 624)
                .Count();
            System.Console.WriteLine(string.Format("Count:{0}", count));

            System.Console.ReadLine();
        }

        /// <summary>
        /// 大量插入数据。
        /// </summary>
        /// <param name="sdb"></param>
        static void Test4(Sequoiadb sdb)
        {
            using (AgileHIS.Entities.DbEntities db = new AgileHIS.Entities.DbEntities())
            {
                var vList = db.HFareDetails.ToList();

                var cs = sdb.GetCollecitonSpace("dbo");
                var coll = cs.GetCollection<HFareDetail>();

                //插入。
                foreach (var item in vList)
                {
                    coll.Insert(item);
                }

                System.Console.WriteLine(string.Format("insert {0} records",vList.Count));
                System.Console.ReadLine();
            }
        }

        static void Test2(Sequoiadb sdb)
        {
            using (AgileHIS.Entities.DbEntities db = new AgileHIS.Entities.DbEntities())
            {
                var v = db.GBCodes.FirstOrDefault();
                BsonDocument doc = new BsonDocument();
                using (SequoiaDB.Bson.IO.BsonWriter w = SequoiaDB.Bson.IO.BsonWriter.Create(doc))
                {
                    SequoiaDB.Bson.Serialization.BsonSerializer.Serialize<GBCode>(w, v);
                }

                var cs = sdb.GetCollecitonSpace("dbo");
                DBCollection coll = null;
                if (!cs.IsCollectionExist("GBCode"))
                    coll = cs.CreateCollection("GBCode");
                else
                    coll = cs.GetCollection("GBCode");

                //插入。
               var vValue=  coll.Insert(doc);
                System.Console.WriteLine(doc);

                //
                BsonDocument matcher = new BsonDocument();
                DBQuery query = new DBQuery();
                query.Matcher = matcher;
                DBCursor cursor = coll.Query(query);
                BsonDocument bson = cursor.Next();
                var v2 = SequoiaDB.Bson.Serialization.BsonSerializer.Deserialize<GBCode>(bson);
                System.Console.WriteLine(bson);
                System.Console.ReadLine();
            }
        }

        /// <summary>
        /// 简单测试。
        /// </summary>
        /// <param name="sdb"></param>
        static void Test1(Sequoiadb sdb)
        {
            if (sdb.IsCollectionSpaceExist("dbo"))
                sdb.DropCollectionSpace("dbo");

            var cs = sdb.CreateCollectionSpace("dbo");
            var coll = cs.CreateCollection("foo");

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
            BsonDocument bson = cursor.Next();
            System.Console.WriteLine(bson);
            System.Console.ReadLine();
        }
    }
}
