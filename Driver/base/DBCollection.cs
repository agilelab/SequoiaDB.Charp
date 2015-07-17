using System.Collections.Generic;
using System;
using SequoiaDB.Bson;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class DBCollection
     *  \brief Database operation interfaces of collection
     */
    public class DBCollection
    {
        private string name;
        private string collectionFullName;
        private CollectionSpace collSpace;
        private IConnection connection;
        internal bool isBigEndian = false;

        //private readonly Logger logger = new Logger("DBCollection");

        /** \property Name
         *  \brief Return the name of current collection
         *  \return The collection name
         */
        public string Name
        {
            get { return name; }
        }

        /** \property FullName
         *  \brief Return the full name of current collection
         *  \return The collection name
         */
        public string FullName
        {
            get { return collectionFullName; }
        }

        /** \property CollSpace
         *  \ brief Return the Collection Space handle of current collection
         *  \return CollectionSpace object
         */
        public CollectionSpace CollSpace
        {
            get { return collSpace; }
        }

        internal IConnection Connection
        {
            get { return connection; }
        }

        internal DBCollection(CollectionSpace cs, string name)
        {
            this.name = name;
            this.collSpace = cs;
            this.collectionFullName = cs.Name + "." + name;
            this.connection = cs.SequoiaDB.Connection;
            this.isBigEndian = cs.isBigEndian;
        }

        /* \fn void Rename(string newName)
         *  \brief Rename the collection
         *  \param newName The new collection name
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        /*
        public void Rename(string newName)
        {
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.RENAME_CMD + " "
                             + SequoiadbConstants.COLLECTION;
            BsonDocument matcher = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_COLLECTIONSPACE, collSpace.Name);
            matcher.Add(SequoiadbConstants.FIELD_OLDNAME, name);
            matcher.Add(SequoiadbConstants.FIELD_NEWNAME, newName);

            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(command, matcher, dummyObj, dummyObj, dummyObj, -1, -1);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
            else
            {
                this.name = newName;
                this.collectionFullName = collSpace.Name + "." + newName;
            }
        }
        */

        /** \fn void Split(string sourceGroupName, string destGroupName,
                           BsonDocument splitCondition, BsonDocument splitEndCondition)
         *  \brief Split the collection from one group into another group by range
         *  \param sourceGroupName The source group
         *  \param destGroupName The destination group
         *  \param splitCondition The split condition
         *  \param splitEndCondition The split end condition or null
         *		eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
    	 *		we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split, 
    	 *		the targe group will get the records whose age's hash value are in [30,60). If splitEndCondition is null,
    	 *		they are in [30,max).
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Split(string sourceGroupName, string destGroupName,
            BsonDocument splitCondition, BsonDocument splitEndCondition)
        {
            // check argument
            if ((null == sourceGroupName || sourceGroupName.Equals("")) ||
                (null == destGroupName || destGroupName.Equals("")) ||
                null == splitCondition)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SPLIT_CMD;
            BsonDocument matcher = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            matcher.Add(SequoiadbConstants.FIELD_SOURCE, sourceGroupName);
            matcher.Add(SequoiadbConstants.FIELD_TARGET, destGroupName);
            matcher.Add(SequoiadbConstants.FIELD_SPLITQUERY, splitCondition);
            if (null != splitEndCondition)
                matcher.Add(SequoiadbConstants.FIELD_SPLITENDQUERY, splitEndCondition);

            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(command, matcher, dummyObj, dummyObj, dummyObj, -1, -1, 0);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void Split(string sourceGroupName, string destGroupName, double percent)
         *  \brief Split the collection from one group into another group by percent
         *  \param sourceGroupName The source group
         *  \param destGroupName The destination group
         *  \param percent percent The split percent, Range:(0.0, 100.0]
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Split(string sourceGroupName, string destGroupName, double percent)
        {
            // check argument
            if ((null == sourceGroupName || sourceGroupName.Equals("")) ||
                (null == destGroupName || destGroupName.Equals("")) ||
                (percent <= 0.0 || percent > 100.0))
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SPLIT_CMD;
            BsonDocument matcher = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            matcher.Add(SequoiadbConstants.FIELD_SOURCE, sourceGroupName);
            matcher.Add(SequoiadbConstants.FIELD_TARGET, destGroupName);
            matcher.Add(SequoiadbConstants.FIELD_SPLITPERCENT, percent);

            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(command, matcher, dummyObj, dummyObj, dummyObj, -1, -1, 0);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn long SplitAsync(String sourceGroupName,
		 *	                    String destGroupName,
		 *	                    BsonDocument splitCondition,
		 *	                    BsonDocument splitEndCondition)
	     *  \brief Split the specified collection from source group to target group by range asynchronously.
         *  \param sourceGroupName the source group name
         *  \param destGroupName the destination group name
         *  \param splitCondition
	     *            the split condition
         *  \param splitEndCondition
	     *            the split end condition or null
	     *            eg:If we create a collection with the option {ShardingKey:{"age":1},ShardingType:"Hash",Partition:2^10},
         *				 we can fill {age:30} as the splitCondition, and fill {age:60} as the splitEndCondition. when split, 
         *			 	 the targe group will get the records whose age's hash values are in [30,60). If splitEndCondition is null,
         *			 	 they are in [30,max).
         *  \return return the task id, we can use the return id to manage the sharding which is run backgroup.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
	     *  \see listTask, cancelTask
	     */
        public long SplitAsync(String sourceGroupName,
                               String destGroupName,
                               BsonDocument splitCondition,
                               BsonDocument splitEndCondition)
        {
            // check argument
            if (sourceGroupName == null || sourceGroupName.Equals("") ||
                destGroupName == null || destGroupName.Equals("") ||
                splitCondition == null)
                throw new BaseException("SDB_INVALIDARG");
            // build a bson to send
            BsonDocument asyncObj = new BsonDocument();
            asyncObj.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            asyncObj.Add(SequoiadbConstants.FIELD_SOURCE, sourceGroupName);
            asyncObj.Add(SequoiadbConstants.FIELD_TARGET, destGroupName);
            asyncObj.Add(SequoiadbConstants.FIELD_SPLITQUERY, splitCondition);
            if (splitEndCondition != null && splitEndCondition.ElementCount != 0)
                asyncObj.Add(SequoiadbConstants.FIELD_SPLITENDQUERY, splitEndCondition);
            asyncObj.Add(SequoiadbConstants.FIELD_ASYNC, true);
            // build run command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SPLIT_CMD;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtnSDBMessage = AdminCommand(commandString, asyncObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
            // build cursor object to get result from database
            DBCursor cursor = new DBCursor(rtnSDBMessage, this);
            BsonDocument result = cursor.Next();
            if (result == null)
                throw new BaseException("SDB_CAT_TASK_NOTFOUND");
            bool flag = result.Contains(SequoiadbConstants.FIELD_TASKID);
            if (!flag)
                throw new BaseException("SDB_CAT_TASK_NOTFOUND");
            long taskid = result.GetValue(SequoiadbConstants.FIELD_TASKID).AsInt64;
            return taskid;
        }

        /** \fn long SplitAsync(String sourceGroupName,
		 *	                    String destGroupName,
		 *	                    double percent)
	     *  \brief Split the specified collection from source group to target group by percent asynchronously.
         *  \param sourceGroupName the source group name
         *  \param destGroupName the destination group name
         *  \param percent
	     *            the split percent, Range:(0,100]
         *  \return return the task id, we can use the return id to manage the sharding which is run backgroup.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
	     *  \see listTask, cancelTask
	     */
        public long SplitAsync(String sourceGroupName,
                               String destGroupName,
                               double percent)
        {
            // check argument
            if (sourceGroupName == null || sourceGroupName.Equals("") ||
                destGroupName == null || destGroupName.Equals("") ||
                percent <= 0.0 || percent > 100.0)
                throw new BaseException("SDB_INVALIDARG");
            // build a bson to send
            BsonDocument asyncObj = new BsonDocument();
            asyncObj.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            asyncObj.Add(SequoiadbConstants.FIELD_SOURCE, sourceGroupName);
            asyncObj.Add(SequoiadbConstants.FIELD_TARGET, destGroupName);
            asyncObj.Add(SequoiadbConstants.FIELD_SPLITPERCENT, percent);
            asyncObj.Add(SequoiadbConstants.FIELD_ASYNC, true);
            // build run command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SPLIT_CMD;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtnSDBMessage = AdminCommand(commandString, asyncObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
            // build cursor object to get result from database
            DBCursor cursor = new DBCursor(rtnSDBMessage, this);
            BsonDocument result = cursor.Next();
            if (result == null)
                throw new BaseException("SDB_CAT_TASK_NOTFOUND");
            bool flag = result.Contains(SequoiadbConstants.FIELD_TASKID);
            if (!flag)
                throw new BaseException("SDB_CAT_TASK_NOTFOUND");
            long taskid = result.GetValue(SequoiadbConstants.FIELD_TASKID).AsInt64;
            return taskid;
        }

        /** \fn ObjectId Insert(BsonDocument insertor)
         *  \brief Insert a document into current collection
         *  \param insertor The Bson document of insertor, can't be null
         *  \return ObjectId
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public BsonValue Insert(BsonDocument insertor)
        {
            if (insertor == null)
                throw new BaseException("SDB_INVALIDARG");
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_INSERT;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.CollectionFullName = collectionFullName;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.Insertor = insertor;

            ObjectId objId;
            BsonValue tmp;
            //if (insertor.
            if (insertor.TryGetValue(SequoiadbConstants.OID, out tmp))
            {
                ;
            }
            else
            {
                objId = ObjectId.GenerateNewId();
                tmp = objId;
                insertor.Add(SequoiadbConstants.OID, objId);
            }

            byte[] request = SDBMessageHelper.BuildInsertRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            return tmp;
        }

        /** \fn void BulkInsert(List<BsonDocument> insertor, int flag)
         *  \brief Insert a bulk of bson objects into current collection
         *  \param insertor The Bson document of insertor list, can't be null
         *  \param flag FLG_INSERT_CONTONDUP or 0
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void BulkInsert(List<BsonDocument> insertor, int flag)
        {
            if (insertor == null || insertor.Count == 0)
                throw new BaseException("SDB_INVALIDARG");
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_INSERT;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.CollectionFullName = collectionFullName;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            if (flag != 0 && flag != SDBConst.FLG_INSERT_CONTONDUP)
                throw new BaseException(flag);
            sdbMessage.Flags = flag;
            sdbMessage.Insertor = insertor[0];

            byte[] request = SDBMessageHelper.BuildInsertRequest(sdbMessage, isBigEndian);

            for (int count = 1; count < insertor.Count; count++)
            {
                request = SDBMessageHelper.AppendInsertMsg(request, insertor[count], isBigEndian);
            }
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void Delete(BsonDocument matcher)
         *  \brief Delete the matching document of current collection
         *  \param matcher The matching condition
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Delete(BsonDocument matcher)
        {
            Delete(matcher, new BsonDocument());
        }

        /** \fn void Delete(BsonDocument matcher, BsonDocument hint)
         *  \brief Delete the matching document of current collection
         *  \param matcher The matching condition
         *  \param hint Hint
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Delete(BsonDocument matcher, BsonDocument hint)
        {
            SDBMessage sdbMessage = new SDBMessage();
            BsonDocument dummyObj = new BsonDocument();
            if (matcher == null)
                matcher = dummyObj;
            if (hint == null)
                hint = dummyObj;

            sdbMessage.OperationCode = Operation.OP_DELETE;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.Flags = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;

            sdbMessage.CollectionFullName = collectionFullName;
            sdbMessage.RequestID = 0;
            sdbMessage.Matcher = matcher;
            sdbMessage.Hint = hint;

            byte[] request = SDBMessageHelper.BuildDeleteRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn void Update(DBQuery query)
         *  \brief Update the document of current collection
         *  \param query DBQuery with matching condition, updating rule and hint
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         *  \note It won't work to update the "ShardingKey" field, but the other fields take effect
         */
        public void Update(DBQuery query)
        {
            _Update(0, query.Matcher, query.Modifier, query.Hint);
        }

        /** \fn void Update(BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
         *  \brief Update the document of current collection
         *  \param matcher The matching condition
         *  \param modifier The updating rule
         *  \param hint Hint
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         *  \note It won't work to update the "ShardingKey" field, but the other fields take effect
         */
        public void Update(BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
        {
            _Update(0, matcher, modifier, hint);
        }

        /** \fn void Upsert(BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
         *  \brief Update the document of current collection, insert if no matching
         *  \param matcher The matching condition
         *  \param modifier The updating rule
         *  \param hint Hint
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         *  \note It won't work to upsert the "ShardingKey" field, but the other fields take effect
         */
        public void Upsert(BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
        {
            _Update(SequoiadbConstants.FLG_UPDATE_UPSERT, matcher, modifier, hint);
        }

        /** \fn DBCursor Query()
         *  \brief Find all documents of current collection
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Query()
        {
            return Query(null, null, null, null, 0, -1);
        }

        /** \fn DBCursor Query(DBQuery query) 
         *  \brief Find documents of current collection with DBQuery
         *  \param query DBQuery with matching condition, selector, order rule, hint, SkipRowsCount and ReturnRowsCount
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Query(DBQuery query)
        {
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument matcher = query.Matcher;
            BsonDocument selector = query.Selector;
            BsonDocument orderBy = query.OrderBy;
            BsonDocument hint = query.Hint;
            long skipRows = query.SkipRowsCount;
            long returnRows = query.ReturnRowsCount;
            int flag = query.Flag;

            return Query(matcher, selector, orderBy, hint, skipRows, returnRows, flag);
        }

        /** \fn DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
         *  \brief Find documents of current collection
         *  \param query The matching condition
         *  \param selector The selective rule
         *  \param orderBy the ordered rule
         *  \param hint One of the indexs in current collection, using default index to query if not provided
         *           eg:{"":"ageIndex"}
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
        {
            return Query(query, selector, orderBy, hint, 0, -1);
        }

        /** \fn DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint, 
         *                     long skipRows, long returnRows) 
         *  \brief Find documents of current collection
         *  \param query The matching condition
         *  \paramselector The selective rule
         *  \param orderBy The ordered rule
         *  \param hint One of the indexs in current collection, using default index to query if not provided
         *           eg:{"":"ageIndex"}
         *  \param skipRows Skip the first numToSkip documents, default is 0
         *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
                              long skipRows, long returnRows)
        {
            return Query(query, selector, orderBy, hint, skipRows, returnRows, 0);
        }

        /** \fn DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint, 
         *                     long skipRows, long returnRows, int flag) 
         *  \brief Find documents of current collection
         *  \param query The matching condition
         *  \paramselector The selective rule
         *  \param orderBy The ordered rule
         *  \param hint One of the indexs in current collection, using default index to query if not provided
         *           eg:{"":"ageIndex"}
         *  \param skipRows Skip the first numToSkip documents, default is 0
         *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
         *  \param flag the flag is used to choose the way to query, the optional options are as below:
         *
         *      DBQuery.FLG_QUERY_FORCE_HINT(0x00000080)      : Force to use specified hint to query, if database have no index assigned by the hint, fail to query
         *      DBQuery.FLG_QUERY_PARALLED(0x00000100)        : Enable paralled sub query
         *      DBQuery.FLG_QUERY_WITH_RETURNDATA(0x00000200) : In general, query won't return data until cursor get from database,
         *                                                      when add this flag, return data in query response, it will be more high-performance
         *
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public virtual DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
                              long skipRows, long returnRows, int flag)
        {
            BsonDocument dummyObj = new BsonDocument();
            if (query == null)
                query = dummyObj;
            if (selector == null)
                selector = dummyObj;
            if (orderBy == null)
                orderBy = dummyObj;
            if (hint == null)
                hint = dummyObj;
            if (returnRows == 0)
            {
                returnRows = -1;
            }
            if (returnRows == 1)
            {
                flag = flag | DBQuery.FLG_QUERY_WITH_RETURNDATA;
            }
            SDBMessage rtnSDBMessage = AdminCommand(collectionFullName, query, selector,
                                                    orderBy, hint, skipRows, returnRows, flag);

            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }

            return new DBCursor(rtnSDBMessage, this);
        }

        /** \fn DBCursor Explain(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
         *                       long skipRows, long returnRows, int flag, BsonDocument options) 
         *  \brief Find documents of current collection
         *  \param query The matching condition
         *  \paramselector The selective rule
         *  \param orderBy The ordered rule
         *  \param hint One of the indexs in current collection, using default index to query if not provided
         *           eg:{"":"ageIndex"}
         *  \param skipRows Skip the first numToSkip documents, default is 0
         *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
         *  \param flag the flag is used to choose the way to query, the optional options are as below:
         *
         *      DBQuery.FLG_QUERY_FORCE_HINT(0x00000080)      : Force to use specified hint to query, if database have no index assigned by the hint, fail to query
         *      DBQuery.FLG_QUERY_PARALLED(0x00000100)        : Enable paralled sub query
         *      DBQuery.FLG_QUERY_WITH_RETURNDATA(0x00000200) : In general, query won't return data until cursor get from database,
         *                                                      when add this flag, return data in query response, it will be more high-performance
         *  \param [in] options The rules of query explain, the options are as below:
         *
         *      Run     : Whether execute query explain or not, true for excuting query explain then get
         *                the data and time information; false for not excuting query explain but get the
         *                query explain information only. e.g. {Run:true}
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Explain(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint,
                                long skipRows, long returnRows, int flag, BsonDocument options)
        {
            BsonDocument newObj = new BsonDocument();
            if (null != hint)
            {
                newObj.Add(SequoiadbConstants.FIELD_HINT, hint);
            }
            if (null != options)
            {
                newObj.Add(SequoiadbConstants.FIELD_OPTIONS, options);
            }

            return Query(query, selector, orderBy, newObj, skipRows, returnRows, flag | DBQuery.FLG_QUERY_EXPLAIN);
        }

        /** \fn DBCursor GetIndexes()
         *  \brief Get all the indexes of current collection
         *  \return A cursor of all indexes or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetIndexes()
        {
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.GET_INXES;
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument obj = new BsonDocument();
            obj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);

            SDBMessage rtn = AdminCommand(commandString, dummyObj, dummyObj, dummyObj, obj, -1, -1, 0);

            int flags = rtn.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }

            return new DBCursor(rtn, this);
        }

        /** \fn DBCursor GetIndex(string name)
         *  \brief Get the named index of current collection
         *  \param name The index name
         *  \return A index, if not exist then return null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetIndex(string name)
        {
            if (name == null)
                return GetIndexes();
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.GET_INXES;
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument obj = new BsonDocument();
            BsonDocument conndition = new BsonDocument();
            obj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            conndition.Add(SequoiadbConstants.IXM_INDEXDEF + "." + SequoiadbConstants.IXM_NAME,
                    name);

            SDBMessage rtn = AdminCommand(commandString, conndition, dummyObj, dummyObj, obj, -1, -1, 0);

            int flags = rtn.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }

            return new DBCursor(rtn, this);
        }

        /** \fn void CreateIndex(string name, BsonDocument key, bool isUnique, bool isEnforced)
         *  \brief Create a index with name and key
         *  \param name The index name
         *  \param key The index key
         *  \param isUnique Whether the index elements are unique or not
         *  \param isEnforced Whether the index is enforced unique.
         *                    This element is meaningful when isUnique is group to true.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void CreateIndex(string name, BsonDocument key, bool isUnique, bool isEnforced)
        {
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_INX;
            BsonDocument obj = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument createObj = new BsonDocument();
            obj.Add(SequoiadbConstants.IXM_NAME, name);
            obj.Add(SequoiadbConstants.IXM_KEY, key);
            obj.Add(SequoiadbConstants.IXM_UNIQUE, isUnique);
            obj.Add(SequoiadbConstants.IXM_ENFORCED, isEnforced);
            createObj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            createObj.Add(SequoiadbConstants.FIELD_INDEX, obj);

            SDBMessage rtn = AdminCommand(commandString, createObj, dummyObj, dummyObj, dummyObj, -1, -1, 0);

            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void DropIndex(string name)
         *  \brief Remove the named index of current collection
         *  \param name The index name
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void DropIndex(string name)
        {
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.DROP_INX;
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument dropObj = new BsonDocument();
            BsonDocument index = new BsonDocument();
            index.Add("", name);
            dropObj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            dropObj.Add(SequoiadbConstants.FIELD_INDEX, index);
            SDBMessage rtn = AdminCommand(commandString, dropObj, dummyObj, dummyObj, dummyObj, -1, -1, 0);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn long GetCount(BsonDocument condition)
         *  \brief Get the count of matching documents in current collection
         *  \param condition The matching rule
         *  \return The count of matching documents
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
        */
        public long GetCount(BsonDocument condition)
        {
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.GET_COUNT;
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument hint = new BsonDocument();
            if (condition == null)
                condition = dummyObj;
            hint.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            SDBMessage rtnSDBMessage = AdminCommand(commandString, condition, dummyObj, dummyObj, hint, 0, -1, 0);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            List<BsonDocument> rtn = GetMoreCommand(rtnSDBMessage);
            return rtn[0][SequoiadbConstants.FIELD_TOTAL].AsInt64;
        }

        /** \fn DBCursor Aggregate(List<BsonDocument> obj)
         *  \brief Execute aggregate operation in specified collection
         *  \param insertor The array of bson objects, can't be null
         *  \return The DBCursor of result or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Aggregate(List<BsonDocument> obj)
        {
            if (obj == null || obj.Count == 0)
                throw new BaseException("SDB_INVALIDARG");
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_AGGREGATE;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.CollectionFullName = collectionFullName;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.Flags = 0;
            sdbMessage.Insertor = obj[0];

            byte[] request = SDBMessageHelper.BuildAggrRequest(sdbMessage, isBigEndian);

            for (int count = 1; count < obj.Count; count++)
            {
                request = SDBMessageHelper.AppendAggrMsg(request, obj[count], isBigEndian);
            }
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }

            return new DBCursor(rtnSDBMessage, this);
        }

        /** \fn DBCursor GetQueryMeta(BsonDocument query, BsonDocument orderBy, BsonDocument hint, 
         *                            long skipRows, long returnRows) 
         *  \brief Get the index blocks' or data blocks' infomations for concurrent query
         *  \param query The matching condition, return the whole range of index blocks if not provided
         *           eg:{"age":{"$gt":25},"age":{"$lt":75}}
         *  \param orderBy The ordered rule, result set is unordered if not provided
         *  \param hint hint One of the indexs in current collection, using default index to query if not provided
         *           eg:{"":"ageIndex"}
         *  \param skipRows Skip the first numToSkip documents, default is 0
         *  \param returnRows Only return numToReturn documents, default is -1 for returning all results
         *  \return The DBCursor of matching infomations or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetQueryMeta(BsonDocument query, BsonDocument orderBy, BsonDocument hint,
                                     long skipRows, long returnRows)
        {
            BsonDocument dummyObj = new BsonDocument();
            if (query == null)
                query = dummyObj;
            if (orderBy == null)
                orderBy = dummyObj;
            if (hint == null)
                hint = dummyObj;
            if (returnRows == 0)
                returnRows = -1;
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.GET_QUERYMETA;
            BsonDocument hint1 = new BsonDocument();
            hint1.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            SDBMessage rtnSDBMessage = AdminCommand(commandString, query, hint, orderBy,
                                                     hint1, skipRows, returnRows, 0);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }
            return new DBCursor(rtnSDBMessage, this);
        }

        /** \fn void AttachCollection (string subClFullName, BsonDocument options)
         * \brief Attach the specified collection.
         * \param subClFullName The name of the subcollection
         * \param options The low boudary and up boudary
         *       eg: {"LowBound":{a:1},"UpBound":{a:100}}
         * \retval void
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public void AttachCollection(string subClFullName, BsonDocument options)
        {
            // check argument
            if (subClFullName == null || subClFullName.Equals("") ||
                subClFullName.Length > SequoiadbConstants.COLLECTION_MAX_SZ ||
                options == null || options.ElementCount == 0)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // build a bson to send
            BsonDocument attObj = new BsonDocument();
            attObj.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            attObj.Add(SequoiadbConstants.FIELD_SUBCLNAME, subClFullName);
            foreach (string key in options.Names)
            {
                attObj.Add(options.GetElement(key));
            }
            // build commond
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LINK_CL;
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtnSDBMessage = AdminCommand(commandString, attObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check the return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void DetachCollection(string subClFullName)
         * \brief Detach the specified collection.
         * \param subClFullName The name of the subcollection
         * \retval void
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public void DetachCollection(string subClFullName)
        {
            // check argument
            if (subClFullName == null || subClFullName.Equals("") ||
                subClFullName.Length > SequoiadbConstants.COLLECTION_MAX_SZ)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // build a bson to send
            BsonDocument detObj = new BsonDocument();
            detObj.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            detObj.Add(SequoiadbConstants.FIELD_SUBCLNAME, subClFullName);
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.UNLINK_CL;
            BsonDocument dummyObj = new BsonDocument();
            // run command
            SDBMessage rtnSDBMessage = AdminCommand(commandString, detObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check the return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void Alter(BsonDocument options)
         * \brief Alter the attributes of current collection
         * \param options The options for altering current collection:
         *
         *     ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
         *     ShardingKey  : Assign the sharding key
         *     ShardingType : Assign the sharding type
         *     Partition    : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20]
         *                    e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
         * \note Can't alter attributes about split in partition collection; After altering a collection to
         *       be a partition collection, need to split this collection manually
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public void Alter(BsonDocument options)
        {
            // check argument
            if (null == options)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // build a bson to send
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_NAME, collectionFullName);
            newObj.Add(SequoiadbConstants.FIELD_OPTIONS, options);
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.ALTER_COLLECTION;
            BsonDocument dummyObj = new BsonDocument();
            // run command
            SDBMessage rtnSDBMessage = AdminCommand(commandString, newObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check the return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn DBCursor ListLobs()
         * \brief List all of the lobs in current collection
         * \retval DBCursor of lobs
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public DBCursor ListLobs()
        {
            DBCursor cursor = null;
            // build command
            string command = SequoiadbConstants.ADMIN_PROMPT
                + SequoiadbConstants.LIST_LOBS_CMD;
            // build a bson to send
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtnSDBMessage = AdminCommand(command, newObj, dummyObj, dummyObj, dummyObj, 0, -1, 0);
            // check the return flag
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                int errCode = new BaseException("SDB_DMS_EOC").ErrorCode;
                if (errCode == flags)
                {
                    return cursor;
                }
                else
                {
                    throw new BaseException(flags);
                }
            }
            cursor = new DBCursor(rtnSDBMessage, this);
            return cursor;
        }

        /** \fn DBLob CreateLob()
         * \brief Create a large object
         * \return The newly created lob object
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public DBLob CreateLob()
        {
            return CreateLob(ObjectId.Empty);
        }

        /** \fn DBLob CreateLob(ObjectId id)
         * \brief Create a large object with specified oid
         * \param id The oid for the creating lob
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public DBLob CreateLob(ObjectId id)
        {
            DBLob lob = new DBLob(this);
            lob.Open(id, DBLob.SDB_LOB_CREATEONLY);
            return lob;
        }

        /** \fn DBLob OpenLob(ObjectId id)
         * \brief Open an existing lob with the speceifed oid
         * \param id The oid of the existing lob
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public DBLob OpenLob(ObjectId id)
        {
            DBLob lob = new DBLob(this);
            lob.Open(id, DBLob.SDB_LOB_READ);
            return lob;
        }

        /** \fn DBLob RemoveLob(ObjectId id)
         * \brief Remove an existing lob with the speceifed oid
         * \param id The oid of the existing lob
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        public void RemoveLob(ObjectId id)
        {
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_COLLECTION, collectionFullName);
            newObj.Add(SequoiadbConstants.FIELD_LOB_OID, id);

            SDBMessage sdbMessage = new SDBMessage();
            // MsgHeader
            sdbMessage.OperationCode = Operation.MSG_BS_LOB_REMOVE_REQ;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            // the rest part of _MsgOpLOb
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = (short)0;
            sdbMessage.Flags = SequoiadbConstants.DEFAULT_FLAGS;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(SequoiadbConstants.DEFAULT_CONTEXTID);
            sdbMessage.Matcher = newObj;


            byte[] request = SDBMessageHelper.BuildRemoveLobRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }

        }

        private void _Update(int flag, BsonDocument matcher, BsonDocument modifier, BsonDocument hint)
        {
            if (modifier == null)
                throw new BaseException("SDB_INVALIDARG");
            BsonDocument dummyObj = new BsonDocument();
            if (matcher == null)
                matcher = dummyObj;
            if (hint == null)
                hint = dummyObj;
            SDBMessage sdbMessage = new SDBMessage();

            sdbMessage.OperationCode = Operation.OP_UPDATE;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.Flags = flag;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.CollectionFullName = collectionFullName;
            sdbMessage.RequestID = 0;
            sdbMessage.Matcher = matcher;
            sdbMessage.Modifier = modifier;
            sdbMessage.Hint = hint;

            byte[] request = SDBMessageHelper.BuildUpdateRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                 throw new BaseException(flags);
        }

        private SDBMessage AdminCommand(string command, BsonDocument query, BsonDocument selector, BsonDocument orderBy,
            BsonDocument hint, long skipRows, long returnRows, int flag)
        {
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.CollectionFullName = command;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.Flags = flag;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.SkipRowsCount = skipRows;
            sdbMessage.ReturnRowsCount = returnRows;
            // matcher
            if (null == query)
            {
                sdbMessage.Matcher = dummyObj;
            }
            else
            {
                sdbMessage.Matcher = query;
            }
            // selector
            if (null == selector)
            {
                sdbMessage.Selector = dummyObj;
            }
            else
            {
                sdbMessage.Selector = selector;
            }
            // orderBy
            if (null == orderBy)
            {
                sdbMessage.OrderBy = dummyObj;
            }
            else
            {
                sdbMessage.OrderBy = orderBy;
            }
            // hint
            if (null == hint)
            {
                sdbMessage.Hint = dummyObj;
            }
            else
            {
                sdbMessage.Hint = hint;
            }

            byte[] request = SDBMessageHelper.BuildQueryRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            return rtnSDBMessage;
        }

        private List<BsonDocument> GetMoreCommand(SDBMessage rtnSDBMessage)
        {
            ulong requestID = rtnSDBMessage.RequestID;
            List<long> contextIDs = rtnSDBMessage.ContextIDList;
            List<BsonDocument> fullList = new List<BsonDocument>();
            bool hasMore = true;
            while (hasMore)
            {
                SDBMessage sdbMessage = new SDBMessage();
                sdbMessage.OperationCode = Operation.OP_GETMORE;
                sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
                sdbMessage.ContextIDList = contextIDs;
                sdbMessage.RequestID = requestID;
                sdbMessage.NumReturned = -1;

                byte[] request = SDBMessageHelper.BuildGetMoreRequest(sdbMessage, isBigEndian);
                connection.SendMessage(request);
                rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
                rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
                int flags = rtnSDBMessage.Flags;
                if (flags != 0)
                    if (flags == SequoiadbConstants.SDB_DMS_EOC)
                        hasMore = false;
                    else
                    {
                        throw new BaseException(flags);
                    }
                else
                {
                    requestID = rtnSDBMessage.RequestID;
                    List<BsonDocument> objList = rtnSDBMessage.ObjectList;
                    fullList.AddRange(objList);
                }
            }
            return fullList;
        }
    }
}
