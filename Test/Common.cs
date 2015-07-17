/******************************************************************************
 *
 * Name: Common.cs
 * Description: Common functions for sample programs
 *              This file does NOT include main function
 *
 ******************************************************************************/

using SequoiaDB;
using SequoiaDB.Bson;
using System;
using System.Collections.Generic;
using SequoiaDB.Driver;

namespace Sample
{
    public class Common
    {
        public static List<BsonDocument> CreateNameList(int listSize)
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
                        {"id", i},

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
            }

            return objList;
        }

        // connect to a given database
        public static void Connect(Sequoiadb sdb)
        {
            try
            {
                // connect to database
                sdb.Connect();
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to connect to database at {0}:{1}", sdb.ServerAddr.Host, sdb.ServerAddr.Port);
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }

        // disconnect from database
        public static void Disconnect(Sequoiadb sdb)
        {
            sdb.Disconnect();
        }

        // get collection space, if the collection space does not exist it will try to create one
        public static CollectionSpace GetCollecitonSpace(Sequoiadb sdb, string csName)
        {
            CollectionSpace cs = null;
            try
            {
                cs = sdb.GetCollecitonSpace(csName);
            }
            catch (BaseException e)
            {
                // verify whether the collection space exists
                if ("SDB_DMS_CS_NOTEXIST" == e.ErrorType)
                {
                    // if the collection space does not exist, we are going to create one
                    cs = CreateCollecitonSpace(sdb, csName);
                }
                else
                {
                    Console.WriteLine("Failed to get collection space {0},ErrorType = {1}", csName, e.ErrorType);
                    Environment.Exit(0);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return cs;
        }

        public static CollectionSpace CreateCollecitonSpace(Sequoiadb sdb, string csName)
        {
            return CreateCollecitonSpace(sdb, csName, SDBConst.SDB_PAGESIZE_DEFAULT);
        }

        // create collection space, if the collection space exists then return
        public static CollectionSpace CreateCollecitonSpace(Sequoiadb sdb, string csName, int pageSize)
        {
            CollectionSpace cs = null;
            try
            {
                cs = sdb.CreateCollectionSpace(csName, pageSize);
            }
            catch (BaseException e)
            {
                // verify whether the collection space exists
                if ("SDB_DMS_CS_EXIST" == e.ErrorType)
                    cs = GetCollecitonSpace(sdb, csName);
                // invalid page size argument
                else if ("SDB_INVALIDARG" == e.ErrorType)
                {
                    Console.WriteLine("Failed to create collection space {0}, invalid page size {1}", csName, pageSize);
                    Environment.Exit(0);
                }
                else
                {
                    Console.WriteLine("Failed to create collection space {0},ErrorType = {1}", csName, e.ErrorType);
                    Environment.Exit(0);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return cs;
        }

        // get collection, if the collection does not exist it will try to create one
        public static DBCollection GetColleciton(CollectionSpace cs, string cName)
        {
            DBCollection dbc = null;
            try
            {
                dbc = cs.GetCollection(cName);
            }
            catch (BaseException e)
            {
                // verify whether the collection space exists
                if ("SDB_DMS_CS_NOTEXIST" == e.ErrorType)
                {
                    cs = CreateCollecitonSpace(cs.SequoiaDB, cs.Name);
                    dbc = GetColleciton(cs, cName);
                }
                // verify whether the collection exists
                else if ("SDB_DMS_NOTEXIST" == e.ErrorType)
                {
                    // if the collection does not exist, we are going to create one
                    dbc = CreateColleciton(cs, cName);
                }
                else
                {
                    Console.WriteLine("Failed to get collection {0},ErrorType = {1}", cName, e.ErrorType);
                    Environment.Exit(0);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return dbc;
        }

        // create collection, if the collection exists then return
        public static DBCollection CreateColleciton(CollectionSpace cs, string cName)
        {
            DBCollection dbc = null;
            try
            {
                dbc = cs.CreateCollection(cName);
            }
            catch (BaseException e)
            {
                // verify whether the collection space exists
                if ("SDB_DMS_CS_NOTEXIST" == e.ErrorType)
                {
                    cs = CreateCollecitonSpace(cs.SequoiaDB, cs.Name);
                    dbc = CreateColleciton(cs, cName);
                }
                // verify whether the collection space exists
                else if ("SDB_DMS_EXIST" == e.ErrorType)
                    dbc = GetColleciton(cs, cName);
                else
                {
                    Console.WriteLine("Failed to create collection {0},ErrorType = {1}", cName, e.ErrorType);
                    Environment.Exit(0);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return dbc;
        }

        public static void FindRecord(DBCollection dbc)
        {
            try
            {
                // find all records from collection
                DBCursor cursor = dbc.Query();

                Console.WriteLine("Record read:");
                while ( cursor.Next() != null )
                    Console.WriteLine(cursor.Current().ToString());
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to find records from collection, ErrorType = {0}", e.ErrorType);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }

        public static void FindRecord(DBCollection dbc, DBQuery query)
        {
            try
            {
                // find specific records from collection with DBQuery
                DBCursor cursor = dbc.Query(query);

                Console.WriteLine("Record read:");
                while ( cursor.Next() != null )
                    Console.WriteLine(cursor.Current().ToString());
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to find records from collection, ErrorType = {0}", e.ErrorType);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }
        
        public static void FindRecord(DBCollection dbc, BsonDocument query, BsonDocument selector, BsonDocument orderBy,
            BsonDocument hint, long skip, long numReturn)
        {
            try
            {
                // find specific records from collection with rules
                DBCursor cursor = dbc.Query(query, selector, orderBy, hint, skip, numReturn);

                Console.WriteLine("Record read:");
                while ( cursor.Next() != null )
                    Console.WriteLine(cursor.Current().ToString());

            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to find records from collection, ErrorType = {0}", e.ErrorType);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }

        public static void GetCount(DBCollection dbc, BsonDocument condition)
        {
            try
            {
                long result = dbc.GetCount(condition);
                Console.WriteLine("There are totally {0} records matches the condition", result);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to get count for query, ErrorType = {0}", e.ErrorType);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }

        public static void DeleteRecords(DBCollection dbc, BsonDocument condition, BsonDocument hint)
        {
            try
            {
                dbc.Delete(condition, hint);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to delete records from collection {0}, ErrorType = {1}", dbc.Name, e.ErrorType);
                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }

        // get index on a given collection
        public static BsonDocument GetIndex(DBCollection dbc, string indexName)
        {
            DBCursor cursor = null;
            try
            {
                cursor = dbc.GetIndex(indexName);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to get index {0} from collection, ErrorType = {1}", indexName, e.ErrorType);
                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }

            return cursor.Next();
        }

        // create index on a given collection
        public static void CreateIndex(DBCollection dbc, string indexName,
                                       BsonDocument key, bool isUnique, bool isEnforced )
        {
            try
            {
                dbc.CreateIndex(indexName, key, isUnique, isEnforced);
            }
            catch (BaseException e)
            {
                Console.WriteLine("Failed to create index, ErrorType = {1}", e.ErrorType);
                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Environment.Exit(0);
            }
        }
    }
}