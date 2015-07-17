using System.Collections.Generic;
using SequoiaDB.Bson;
using System;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class CollectionSpace
     *  \brief Database operation interfaces of collection space
     */
    public class CollectionSpace
    {
        private string name;
        private Sequoiadb sdb;
        internal bool isBigEndian = false;

        /** \property Name
         *  \brief Return the name of current collection space
         *  \return The collection space name
         */
        public string Name
        {
            get { return name; }
        }

        /** \property SequoiaDB
         *  \brief Return the Sequoiadb handle of current collection space
         *  \return Sequoiadb object
         */
        public Sequoiadb SequoiaDB
        {
            get { return sdb; }
        }

        internal CollectionSpace(Sequoiadb sdb, string name)
        {
            this.name = name;
            this.sdb = sdb;
            this.isBigEndian = sdb.isBigEndian;
        }

        /** \fn DBCollection GetCollection(string collectionName)
         *  \brief Get the named collection
         *  \param collectionName The collection name
         *  \return The DBCollection handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCollection GetCollection(string collectionName)
        {
            if (IsCollectionExist(collectionName))
                return new DBCollection(this, collectionName.Trim());
            else
                throw new BaseException("SDB_DMS_NOTEXIST");
        }

        /// <summary>
        /// Get DBCollection。
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <returns></returns>
        public DBCollection<TDocument> GetCollection<TDocument>()
        {
            string collectionName = typeof(TDocument).Name;
            return GetCollection<TDocument>(collectionName);
        }

        /// <summary>
        /// Get DBCollection。
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public DBCollection<TDocument> GetCollection<TDocument>(string collectionName)
        {
            if (!IsCollectionExist(collectionName))
            {
                CreateCollection(collectionName);
            }
            return new DBCollection<TDocument>(this, collectionName.Trim());
        }

        /** \fn bool IsCollectionExist(string colName)
         *  \brief Verify the existence of collection in current colleciont space
         *  \param colName The collection name
         *  \return True if collection existed or False if not existed
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public bool IsCollectionExist(string colName)
        {
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.TEST_CMD + " "
                             + SequoiadbConstants.COLLECTION;
            BsonDocument condition = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_NAME, this.name + "." + colName);
            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags == 0)
                return true;
            else if (flags == (int)Errors.errors.SDB_DMS_NOTEXIST)
                return false;
            else
                throw new BaseException(flags);
        }

        /** \fn DBCollection CreateCollection(string collectionName)
         *  \brief Create the named collection in current collection space
         *  \param collectionName The collection name
         *  \return The DBCollection handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCollection CreateCollection(string collectionName)
        {
            SDBMessage rtn = AdminCommand(SequoiadbConstants.CREATE_CMD, SequoiadbConstants.COLLECTION,
                name + "." + collectionName);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            return new DBCollection(this, collectionName.Trim());
        }

        /** \fn DBCollection CreateCollection(string collectionName, BsonDocument options)
         *  \brief Create the named collection in current collection space
         *  \param collectionName The collection name
         *  \param options The options
         *  \return The DBCollection handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCollection CreateCollection(string collectionName, BsonDocument options)
        {
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_CMD + " "
                             + SequoiadbConstants.COLLECTION;
            BsonDocument cObj = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();

            cObj.Add(SequoiadbConstants.FIELD_NAME, name + "." + collectionName);
            if (options != null && options.ElementCount != 0)
            {
                foreach (string key in options.Names)
                {
                    cObj.Add(options.GetElement(key));
                }
            }
            //cObj.Add(SequoiadbConstants.FIELD_SHARDINGKEY, options[SequoiadbConstants.FIELD_SHARDINGKEY]);

            SDBMessage rtn = AdminCommand(command, cObj, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            return new DBCollection(this, collectionName.Trim());
        }

        /** \fn void DropCollection(string collectionName)
         *  \brief Remove the named collection of current collection space
         *  \param collectionName The collection name
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void DropCollection(string collectionName)
        {
            SDBMessage rtn = AdminCommand(SequoiadbConstants.DROP_CMD, SequoiadbConstants.COLLECTION,
                name + "." + collectionName);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        //public DBCollection GetCollection(string collectionName) 

        private SDBMessage AdminCommand(string cmdType, string contextType, string contextName)
        {
            IConnection connection = sdb.Connection;
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage sdbMessage = new SDBMessage();
            string commandString = SequoiadbConstants.ADMIN_PROMPT + cmdType + " " + contextType;

            BsonDocument cObj = new BsonDocument();
            cObj.Add(SequoiadbConstants.FIELD_NAME, contextName);
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.Matcher = cObj;
            sdbMessage.CollectionFullName = commandString;
            sdbMessage.Flags = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.SkipRowsCount = -1;
            sdbMessage.ReturnRowsCount = -1;
            sdbMessage.Selector = dummyObj;
            sdbMessage.OrderBy = dummyObj;
            sdbMessage.Hint = dummyObj;

            byte[] request = SDBMessageHelper.BuildQueryRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);

            return rtnSDBMessage;
        }

        private SDBMessage AdminCommand(string command, BsonDocument matcher, BsonDocument selector,
                                         BsonDocument orderBy, BsonDocument hint)
        {
            BsonDocument dummyObj = new BsonDocument();
            IConnection connection = sdb.Connection;
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.CollectionFullName = command;
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.Flags = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.SkipRowsCount = 0;
            sdbMessage.ReturnRowsCount = -1;
            // matcher
            if (null == matcher)
            {
                sdbMessage.Matcher = dummyObj;
            }
            else
            {
                sdbMessage.Matcher = matcher;
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
            SDBMessage rtnMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnMessage);
            return rtnMessage;
        }
    }
}
