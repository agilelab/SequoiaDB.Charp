using System.Collections.Generic;
using SequoiaDB.Bson;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Zhaobo Tan
 */
namespace SequoiaDB.Driver
{
    /** \class Domain
     *  \brief Database operation interfaces of domain
     */
	public class Domain
	{
        private string name;
        private Sequoiadb sdb;
        internal bool isBigEndian = false;

        /** \property Name
         *  \brief Return the name of current domain
         *  \return The domain name
         */
        public string Name
        {
            get { return name; }
        }

        /** \property SequoiaDB
         *  \brief Return the Sequoiadb handle of current domain
         *  \return Sequoiadb object
         */
        public Sequoiadb SequoiaDB
        {
            get { return sdb; }
        }

        internal Domain(Sequoiadb sdb, string name)
        {
            this.name = name;
            this.sdb = sdb;
            this.isBigEndian = sdb.isBigEndian;
        }

        /** \fn void Alter(BsonDocumet options)
         *  \brief Alter current domain.
         *  \param [in] options The options user wants to alter
         *  
         *      Groups:    The list of replica groups' names which the domain is going to contain.
         *                 eg: { "Groups": [ "group1", "group2", "group3" ] }, it means that domain
         *                 changes to contain "group1", "group2" and "group3".
         *                 We can add or remove groups in current domain. However, if a group has data
         *                 in it, remove it out of domain will be failing.
         *      AutoSplit: Alter current domain to have the ability of automatically split or not. 
         *                 If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
         *                 the data of this collection will be split(hash split) into all the groups in this domain automatically.
         *                 However, it won't automatically split data into those groups which were add into this domain later.
         *                 eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Alter(BsonDocument options)
        {
            // check
            if (null == options)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // append argument
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_NAME, this.name);
            newObj.Add(SequoiadbConstants.FIELD_OPTIONS, options);
            // cmd
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.ALTER_CMD + " "
                             + SequoiadbConstants.DOMAIN;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(command, newObj, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn DBCursor ListCS()
         *  \brief List all the collection spaces in current domain.
         *  \return The DBCursor of result
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListCS()
        {
            return ListCSCL(SDBConst.SDB_LIST_CS_IN_DOMAIN);
        }

        /** \fn DBCursor listCL()
         *  \brief List all the collections in current domain.
         *  \return The DBCursor of result
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListCL()
        {
            return ListCSCL(SDBConst.SDB_LIST_CL_IN_DOMAIN);
        }

        private DBCursor ListCSCL(int type)
        {
            // append argument
            BsonDocument matcher = new BsonDocument();
            BsonDocument selector = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_DOMAIN, this.name);
            selector.Add(SequoiadbConstants.FIELD_NAME, null);
            // get cs or cl in current domain 
            DBCursor cursor = this.sdb.GetList(type, matcher, selector, null);
            return cursor;
        }

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
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            return rtnSDBMessage;
        }

	}
}
