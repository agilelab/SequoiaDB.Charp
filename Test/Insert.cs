/******************************************************************************
 *
 * Name: Insert.cs
 * Description: This program demostrates how to use the C#.Net Driver
 *              This program will also populate some testing data and create
 *              indexes
 * Notice:
 *              Please add reference of 'sdbcs.dll' in your project.
 *
 * ****************************************************************************/

// Please import namespace SequoiaDB and SequoiaDB.Bson
using SequoiaDB;
using SequoiaDB.Bson;
using System;


namespace Sample
{
    class Insert
    {
        /* create english record */
        static BsonDocument CreateEnglisthRecord()
        {
            BsonDocument obj = new BsonDocument();
            try
            {
                obj.Add("name", "tom");
                obj.Add("age", 60);
                obj.Add("id", 2000);

                // an embedded bson object
                BsonDocument phone = new BsonDocument
                {
                    {"0", "1808835242"},
                    {"1", "1835923246"}
                };

                obj.Add("phone", phone);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create record.");
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return obj;
        }

        static BsonDocument CreateChineseRecord()
        {
            BsonDocument obj = new BsonDocument();
            try
            {
                obj.Add("姓名", "杰克");
                obj.Add("年龄", 70);
                obj.Add("id", 2001);

                // an embedded bson object
                BsonDocument phone = new BsonDocument
                {
                    {"0", "1808835242"},
                    {"1", "1835923246"}
                };

                obj.Add("电话", phone);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create record.");
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return obj;
        }
           
        public static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Please give the database server address <IP:Port>");
                Environment.Exit(0);
            }

            // The database server address
            string sdbIP = args[0];
            // The collection space name
            string csName = "SAMPLE";
            // The collection name
            string cName = "employee";

            BsonDocument insertor1 = CreateEnglisthRecord();
            BsonDocument insertor2 = CreateChineseRecord();

            Sequoiadb sdb = new Sequoiadb(sdbIP);

            Common.Connect(sdb);
            CollectionSpace cs = Common.GetCollecitonSpace(sdb, csName);
            DBCollection dbc = Common.GetColleciton(cs, cName);

            try
            {
                ObjectId id = dbc.Insert(insertor1);
                Console.WriteLine("Successfully inserted english records, object ID = {0}", id.ToString());
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to insert english record, ErrorType = {0}", e.ErrorType);
                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            try
            {
                ObjectId id = dbc.Insert(insertor2);
                Console.WriteLine("Successfully inserted chinese records, object ID = {0}", id.ToString());
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to insert chinese record, ErrorType = {0}", e.ErrorType);
                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            Common.Disconnect(sdb);
        }
    }
}
