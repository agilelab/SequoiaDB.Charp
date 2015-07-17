/******************************************************************************
 *
 * Name: Find.cs
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
using System.Collections.Generic;
using SequoiaDB.Driver;

namespace Sample
{
    class Find
    {
        // index name
        private const string indexName = "employee_id";
        // records count to be inserted
        private const int numRecord = 100;

        static List<BsonDocument> CreateNameList(int listSize)
        {
            List<BsonDocument> objList = new List<BsonDocument>();
            if (listSize <= 0)
                return objList;

            try
            {
                for (int i = 0; i < listSize; ++i)
                {
                    BsonDocument obj = new BsonDocument
                    {
                        {"FirstName", "John"},
                        {"LastName", "Smith"},
                        {"Age", 50},
                        {"Id", i},

                        {"Address", 
                            new BsonDocument
                            {
                                {"StreetAddress", "21 2nd Street"},
                                {"City", "NewYork"},
                                {"State", "NY"},
                                {"PostalCode", "10021"}
                            }
                        },

                        {"PhoneNumber",
                            new BsonDocument
                            {
                                {"Type", "Home"},
                                {"Number", "212 555-1234"}
                            }
                        }
                    };

                    objList.Add(obj);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create name list records.");
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return objList;
        }

        static void FindSpecificRecord(DBCollection dbc)
        {
            BsonDocument query = null;
            try
            {
                query = new BsonDocument { { "Id", 50 } };
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create specific record query condition");
                Console.WriteLine(e.Message);
                return;
            }

            // new an empty bson object
            BsonDocument dummy = new BsonDocument();
            Console.WriteLine("Find specific record from collection");
            Common.FindRecord(dbc, query, dummy, dummy, dummy, 0, -1);
        }

        static void FindAllRecords(DBCollection dbc)
        {
            Console.WriteLine("Find all records from collection");
            Common.FindRecord(dbc);
        }

        static void FindRangeRecords(DBCollection dbc)
        {
            DBQuery query = new DBQuery();
            try 
            {
                BsonDocument condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 25},
                            {"$lte", 30}
                        }
                    }
                };

                query.Matcher = condition;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create range record query condition");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Find range records from collection");
            Common.FindRecord(dbc, query);
        }

        static void FindRecordsUsingHintIXScan(DBCollection dbc)
        {
            DBQuery query = new DBQuery();
            try
            {
                BsonDocument condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 50},
                            {"$lte", 70}
                        }
                    }
                };

                query.Matcher = condition;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create query condition");
                Console.WriteLine(e.Message);
                return;
            }

            try
            {
                // provide index name for index scan
                BsonDocument hint = new BsonDocument { { "", indexName } };

                query.Hint = hint;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create hint");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Find records using index scan");
            Common.FindRecord(dbc, query);
        }

        static void FindRecordsUsingHintCScan(DBCollection dbc)
        {
            DBQuery query = new DBQuery();
            try
            {
                BsonDocument condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 50},
                            {"$lte", 70}
                        }
                    }
                };

                query.Matcher = condition;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create query condition");
                Console.WriteLine(e.Message);
                return;
            }

            try
            {
                // provide NULL for collection scan
                BsonDocument hint = new BsonDocument { { "", "" } };

                query.Hint = hint;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create hint");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Find records using collection scan");
            Common.FindRecord(dbc, query);
        }

        static void FindSortedRecords(DBCollection dbc)
        {
            DBQuery query = new DBQuery();
            try
            {
                BsonDocument condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 50},
                            {"$lte", 70}
                        }
                    }
                };

                query.Matcher = condition;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create query condition");
                Console.WriteLine(e.Message);
                return;
            }

            try
            {
                BsonDocument orderBy = new BsonDocument { { "id", -1 } };

                query.OrderBy = orderBy;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create orderBy clause");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Find ordered data ( by \"id\" in DESC order ) from Collection");
            Common.FindRecord(dbc, query);
        }

        static void FindRecordsFields(DBCollection dbc)
        {
            DBQuery query = new DBQuery();
            try
            {
                BsonDocument condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 50},
                            {"$lte", 70}
                        }
                    }
                };

                query.Matcher = condition;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create query condition");
                Console.WriteLine(e.Message);
                return;
            }

            try
            {
                BsonDocument selector = new BsonDocument 
                { 
                    {"id", ""},
                    {"FirstName", ""},
                    {"LastName", ""},
                    {"PhoneNumber", ""}
                };

                query.Selector = selector;
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create fields selector");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Find specific fields from Collection");
            Common.FindRecord(dbc, query);
        }

        static void FindTotalNumRecords(DBCollection dbc)
        {
            BsonDocument condition = new BsonDocument();
            Console.WriteLine("Get total number of records for collection");
            Common.GetCount(dbc, condition);
        }

        static void GetNumofRowsCondition(DBCollection dbc)
        {
            BsonDocument condition = null;
            try
            {
                condition = new BsonDocument
                {
                    {"Id",
                        new BsonDocument
                        {
                            {"$gte", 50},
                            {"$lte", 70}
                        }
                    }
                };
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to create query condition");
                Console.WriteLine(e.Message);
                return;
            }

            Console.WriteLine("Get count for query");
            Common.GetCount(dbc, condition);
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

            Sequoiadb sdb = new Sequoiadb(sdbIP);

            Common.Connect(sdb);
            CollectionSpace cs = Common.GetCollecitonSpace(sdb, csName);
            DBCollection dbc = Common.GetColleciton(cs, cName);

            // delete all records from the collection
            BsonDocument bson = new BsonDocument();
            Common.DeleteRecords(dbc, bson, bson);

            // if index does not exist, create one
            if (null == Common.GetIndex(dbc, indexName))
            {
                BsonDocument key = null;
                try
                {
                    key = new BsonDocument { { "Id", 1 } };
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to create index def key");
                    Console.WriteLine(e.Message);
                    Environment.Exit(0);
                }

                Common.CreateIndex(dbc, indexName, key, true, true);
                Console.WriteLine("Index {0} has been successfully created", indexName);
            }
            else
            {
                Console.WriteLine("Found index {0} already exist", indexName);
            }

            // create name list
            List<BsonDocument> objList = CreateNameList(numRecord);
            // insert obj
            for (int i = 0; i < objList.Count; ++i)
            {
                try
                {
                    dbc.Insert(objList[i]);
                }
                catch (BaseException e)
                {
                    Console.WriteLine("Failed to insert record, ErrorType = {0}", e.ErrorType);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }

            FindSortedRecords(dbc);
            FindAllRecords(dbc);
            FindRangeRecords(dbc);
            FindRecordsUsingHintIXScan(dbc);
            FindRecordsUsingHintCScan(dbc);
            FindSortedRecords(dbc);
            FindRecordsFields(dbc);
            FindTotalNumRecords(dbc);
            GetNumofRowsCondition(dbc);

            Common.Disconnect(sdb);
        }
    }
}
