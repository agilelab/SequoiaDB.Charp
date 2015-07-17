using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System;
using SequoiaDB.Bson;
using System.IO;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class Sequoiadb
     *  \brief Database operation interfaces of admin
     */
    public class Sequoiadb
    {
        private ServerAddress serverAddress = null;
        private ServerAddress[] serverAddresses = null;
        private IConnection connection = null;
        private string userName = "";
        private string password = "";
        internal bool isBigEndian = false;
        //private static readonly Logger logger = new Logger("Sequoiadb");

        public IConnection Connection
        {
            get { return connection; }
        }

        /** \property ServerAddr
         *  \brief Return or group the remote server address
         *  \return The ServerAddress
         */
        public ServerAddress ServerAddr
        {
            get { return serverAddress; }
            set { serverAddress = value; }
        }

        /** \fn Sequoiadb()
         *  \brief Default Constructor
         *  
         * Server address "127.0.0.1 : 11810"
         */
        public Sequoiadb()
        {
            serverAddress = new ServerAddress();
        }

        /** \fn Sequoiadb(string connString)
         *  \brief Constructor
         *  \param connString Remote server address "IP : Port" or "IP"(port is 11810)
         */
        public Sequoiadb(string connString)
        {
            serverAddress = new ServerAddress(connString);
        }

        /** \fn Sequoiadb(List<string> connStrings)
         *  \brief Constructor
         *  \param connStrings Remote server addresses "IP : Port"
         */
        public Sequoiadb(List<string> connStrings)
        {
            if (connStrings.Count == 0)
                throw new BaseException("SDB_INVALIDARG");
            serverAddresses = new ServerAddress[connStrings.Count];
            for (int i = 0; i < connStrings.Count; i++)
            {
                try
                {
                    serverAddresses[i] = new ServerAddress(connStrings[i]);
                }
                catch (BaseException e)
                {
                    throw e;
                }
            }
        }

        /** \fn Sequoiadb(string host, int port)
         *  \brief Constructor
         *  \param addr IP address
         *  \param port Port
         */
        public Sequoiadb(string host, int port)
        {
            serverAddress = new ServerAddress(host, port);
        }

        /** \fn void Connect()
         *  \brief Connect to remote Sequoiadb database server
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Connect()
        {
            Connect(userName, password);
        }

        /** \fn void Connect(string username, string password)
         *  \brief Connect to remote Sequoiadb database server
         *  \username Sequoiadb connection user name
         *  \password Sequoiadb connection password
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Connect(string username, string password)
        {
            this.userName = username;
            this.password = password;
            Connect(username, password, null);
        }

        /** \fn void Connect(string username, string password, ConfigOptions options)
         *  \brief Connect to remote Sequoiadb database server
         *  \username Sequoiadb connection user name
         *  \password Sequoiadb connection password
         *  \options The options for connection
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Connect(string username, string password, ConfigOptions options)
        {
            ConfigOptions opts = options;
            if (username == null)
                username = "";
            if (password == null)
                password = "";
            this.userName = username;
            this.password = password;
            if (options == null)
                opts = new ConfigOptions();
            if (connection == null)
            {
                // single address
                if (serverAddress != null)
                {
                    // connect
                    try
                    {
                        connection = new ConnectionTCPImpl(serverAddress, opts);
                        connection.Connect();
                    }
                    catch (System.Exception e)
                    {
                        connection = null;
                        throw e;
                    }
                }
                // several addresses
                else if (serverAddresses != null)
                {
                    int size = serverAddresses.Length;
                    Random random = new Random();
                    int count = random.Next(size);
                    int mark = count;
                    do
                    {
                        count = ++count % size;
                        try
                        {
                            ServerAddress conn = serverAddresses[count];
                            connection = new ConnectionTCPImpl(conn, opts);
                            connection.Connect();
                        }
                        catch (System.Exception)
                        {
                            if (mark == count)
                            {
                                throw new BaseException("SDB_NET_CANNOT_CONNECT");
                            }
                            continue;
                        }
                        break;
                    } while (mark != count);
                }
                else
                {
                    throw new BaseException("SDB_NET_CANNOT_CONNECT");
                }
                // get endian info
                isBigEndian = RequestSysInfo();
                // authentication
                try
                {
                    Auth();
                }
                catch (BaseException e)
                {
                    throw e;
                }
            }
        }

        /** \fn Disconnect()
         *  \brief Disconnect the remote server
         *  \return void
         *  \exception System.Exception
         */
        public void Disconnect()
        {
            byte[] request = SDBMessageHelper.BuildDisconnectRequest(isBigEndian);
            connection.SendMessage(request);
            connection.Close();
            connection = null;
        }

        /** \fn bool IsClosed()
         *  \brief Judge wether the connection is closed
         *  \return If the connection is closed, return true
         *  \exception System.Exception
         */
        private bool IsClosed()
        {
            if (connection == null)
                return true;
            return connection.IsClosed();
        }

        /** \fn bool IsValid()
         *  \brief Judge wether the connection is is valid or not
         *  \return If the connection is valid, return true
         */
        public bool IsValid()
        {
            if (connection == null || connection.IsClosed())
            {
                return false;
            }
            int flags = -1;
            // send a lightweight query msg to engine to check
            // wether connection is ok or not
            try
            {
                SDBMessage sdbMessage = new SDBMessage();
                sdbMessage.OperationCode = Operation.OP_KILL_CONTEXT;
                sdbMessage.ContextIDList = new List<long>();
                sdbMessage.ContextIDList.Add(-1);
                byte[] request = SDBMessageHelper.BuildKillCursorMsg(sdbMessage, isBigEndian);

                connection.SendMessage(request);
                SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
                rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
                flags = rtnSDBMessage.Flags;
                if (flags != 0)
                {
                    throw new BaseException(flags);
                }
            }
            catch (System.Exception)
            {
                return false;
            }
            return true;
        }

        /** \fn void CreateUser(string username, string password)
         *  \brief Add an user in current database
         *  \username Sequoiadb connection user name
         *  \password Sequoiadb connection password
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void CreateUser(string username, string password)
        {
            if (username == null || password == null)
                throw new BaseException("SDB_INVALIDARG");
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.MSG_AUTH_CRTUSR_REQ;
            sdbMessage.RequestID = 0;
            MD5 md5 = MD5.Create();
            byte[] data = md5.ComputeHash(Encoding.Default.GetBytes(password));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
                builder.Append(data[i].ToString("x2"));
            byte[] request = SDBMessageHelper.BuildAuthCrtMsg(sdbMessage, username, builder.ToString(), isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void RemoveUser(string username, string password)
         *  \brief Remove the user from current database
         *  \username Sequoiadb connection user name
         *  \password Sequoiadb connection password
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void RemoveUser(string username, string password)
        {
            if (username == null || password == null)
                throw new BaseException("SDB_INVALIDARG");
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.MSG_AUTH_DELUSR_REQ;
            sdbMessage.RequestID = 0;
            MD5 md5 = MD5.Create();
            byte[] data = md5.ComputeHash(Encoding.Default.GetBytes(password));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
                builder.Append(data[i].ToString("x2"));
            byte[] request = SDBMessageHelper.BuildAuthDelMsg(sdbMessage, username, builder.ToString(), isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void TransactionBegin()
         *  \brief Begin the database transaction
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void TransactionBegin()
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_TRANS_BEGIN;
            sdbMessage.RequestID = 0;
            byte[] request = SDBMessageHelper.BuildTransactionRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void TransactionCommit()
         *  \brief Commit the database transaction
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void TransactionCommit()
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_TRANS_COMMIT;
            sdbMessage.RequestID = 0;
            byte[] request = SDBMessageHelper.BuildTransactionRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void TransactionRollback()
         *  \brief Rollback the database transaction
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void TransactionRollback()
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_TRANS_ROLLBACK;
            sdbMessage.RequestID = 0;
            byte[] request = SDBMessageHelper.BuildTransactionRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void ChangeConnectionOptions(ConfigOptions opts)
         *  \brief Change the connection options
         *  \return void
         *  \param opts The connection options
         *  \exception System.Exception
         */
        public void ChangeConnectionOptions(ConfigOptions opts)
        {
            connection.ChangeConfigOptions(opts);
            try
            {
                Auth();
            }
            catch (BaseException e)
            {
                throw e;
            }

        }

        /** \fn CollectionSpace CreateCollectionSpace(string csName)
         *  \brief Create the named collection space with default SDB_PAGESIZE_64K
         *  \param csName The collection space name
         *  \return The collection space handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public CollectionSpace CreateCollectionSpace(string csName)
        {
            return CreateCollectionSpace(csName, SDBConst.SDB_PAGESIZE_DEFAULT);
        }

        /** \fn CollectionSpace CreateCollectionSpace(string csName, int pageSize)
         *  \brief Create the named collection space
         *  \param csName The collection space name
         *  \param pageSize The Page Size as below
         *  
         *        SDB_PAGESIZE_4K
         *        SDB_PAGESIZE_8K
         *        SDB_PAGESIZE_16K
         *        SDB_PAGESIZE_32K
         *        SDB_PAGESIZE_64K
         *        SDB_PAGESIZE_DEFAULT
         *  \return The collection space handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public CollectionSpace CreateCollectionSpace(string csName, int pageSize)
        {
            if (csName == null ||
                pageSize != SDBConst.SDB_PAGESIZE_4K &&
                pageSize != SDBConst.SDB_PAGESIZE_8K &&
                pageSize != SDBConst.SDB_PAGESIZE_16K &&
                pageSize != SDBConst.SDB_PAGESIZE_32K &&
                pageSize != SDBConst.SDB_PAGESIZE_64K &&
                pageSize != SDBConst.SDB_PAGESIZE_DEFAULT)
            {
                throw new BaseException("SDB_INVALIDARG");
            }

            BsonDocument options = new BsonDocument();
            options.Add(SequoiadbConstants.FIELD_PAGESIZE, pageSize);
            SDBMessage rtn = CreateCS(csName, options);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            return new CollectionSpace(this, csName.Trim());
        }

        /** \fn CollectionSpace CreateCollectionSpace(string csName, BsonDocument options)
         *  \brief Create the named collection space
         *  \param csName The collection space name
         *  \param options The options specified by user, e.g. {"PageSize": 4096, "Domain": "mydomain"}
         *  
         *      PageSize   : Assign the pagesize of the collection space
         *      Domain     : Assign which domain does current collection space belong to
         *  \return The collection space handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public CollectionSpace CreateCollectionSpace(string csName, BsonDocument options)
        {
            if (csName == null)
            {
                throw new BaseException("SDB_INVALIDARG");
            }

            SDBMessage rtn = CreateCS(csName, options);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            return new CollectionSpace(this, csName.Trim());
        }

        /** \fn void DropCollectionSpace(string csName)
         *  \brief Remove the named collection space
         *  \param csName The collection space name
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void DropCollectionSpace(string csName)
        {
            SDBMessage rtn = AdminCommand(SequoiadbConstants.DROP_CMD, SequoiadbConstants.COLSPACE, csName);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn CollectionSpace GetCollecitonSpace(string csName)
         *  \brief Get the named collection space
         *  \param csName The collection space name
         *  \return The CollecionSpace handle
         *  \note If collection space not exit, throw BaseException
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public CollectionSpace GetCollecitonSpace(string csName)
        {
            if (IsCollectionSpaceExist(csName))
                return new CollectionSpace(this, csName.Trim());
            else
                throw new BaseException("SDB_DMS_CS_NOTEXIST");
        }

        /** \fn bool IsCollectionSpaceExist(string csName)
         *  \brief Verify the existence of collection space
         *  \param csName The collecion space name
         *  \return True if existed or False if not existed
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public bool IsCollectionSpaceExist(string csName)
        {
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.TEST_CMD + " "
                             + SequoiadbConstants.COLSPACE;
            BsonDocument condition = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_NAME, csName);
            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags == 0)
                return true;
            else if (flags == (int)Errors.errors.SDB_DMS_CS_NOTEXIST)
                return false;
            else
                throw new BaseException(flags);
        }
        /** \fn DBCursor ListCollectionSpaces()
         *  \brief List all the collecion space
         *  \return A DBCursor of all the collection space or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListCollectionSpaces()
        {
            return GetList(SDBConst.SDB_LIST_COLLECTIONSPACES);
        }

        /** \fn DBCursor ListCollections()
         *  \brief List all the collecion space
         *  \return A DBCursor of all the collection or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListCollections()
        {
            return GetList(SDBConst.SDB_LIST_COLLECTIONS);
        }

        /** \fn DBCursor Exec(string sql)
         *  \brief Executing SQL command
         *  \param sql SQL command
         *  \return The DBCursor of matching documents or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor Exec(string sql)
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_SQL;
            sdbMessage.RequestID = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;

            byte[] request = SDBMessageHelper.BuildSqlMsg(sdbMessage, sql, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }
            }
            if (null == rtnSDBMessage.ContextIDList ||
                  rtnSDBMessage.ContextIDList.Count != 1 ||
                  rtnSDBMessage.ContextIDList[0] == -1)
                return null;
            return new DBCursor(rtnSDBMessage, this);
        }

        /** \fn void ExecUpdate(string sql)
         *  \brief Executing SQL command for updating
         *  \param sql SQL command
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void ExecUpdate(string sql)
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_SQL;
            sdbMessage.RequestID = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;

            byte[] request = SDBMessageHelper.BuildSqlMsg(sdbMessage, sql, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn DBCursor GetSnapshot(int snapType, BsonDocument matcher, BsonDocument selector,
                                          BsonDocument orderBy)
         *  \brief Get the snapshots of specified type
         *  \param snapType The specified type as below:
         *  
         *      SDB_SNAP_CONTEXTS
         *      SDB_SNAP_CONTEXTS_CURRENT
         *      SDB_SNAP_SESSIONS
         *      SDB_SNAP_SESSIONS_CURRENT
         *      SDB_SNAP_COLLECTIONS
         *      SDB_SNAP_COLLECTIONSPACES
         *      SDB_SNAP_DATABASE
         *      SDB_SNAP_SYSTEM
         *      SDB_SNAP_CATALOG
         *  \param matcher The matching condition or null
         *  \param selector The selective rule or null
         *  \param orderBy The ordered rule or null
         *  \return A DBCursor of all the fitted objects or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetSnapshot(int snapType, BsonDocument matcher, BsonDocument selector,
                                          BsonDocument orderBy)
        {
            string command = null;
            switch (snapType)
            {
                case SDBConst.SDB_SNAP_CONTEXTS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.CONTEXTS;
                    break;
                case SDBConst.SDB_SNAP_CONTEXTS_CURRENT:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.CONTEXTS_CUR;
                    break;
                case SDBConst.SDB_SNAP_SESSIONS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.SESSIONS;
                    break;
                case SDBConst.SDB_SNAP_SESSIONS_CURRENT:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.SESSIONS_CUR;
                    break;
                case SDBConst.SDB_SNAP_COLLECTIONS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.COLLECTIONS;
                    break;
                case SDBConst.SDB_SNAP_COLLECTIONSPACES:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.COLSPACES;
                    break;
                case SDBConst.SDB_SNAP_DATABASE:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.DATABASE;
                    break;
                case SDBConst.SDB_SNAP_SYSTEM:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.SYSTEM;
                    break;
                case SDBConst.SDB_SNAP_CATALOG:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " " +
                           SequoiadbConstants.CATA;
                    break;
                default:
                    throw new BaseException("SDB_INVALIDARG");
            }

            BsonDocument dummyObj = new BsonDocument();
            if (matcher == null)
                matcher = dummyObj;
            if (selector == null)
                selector = dummyObj;
            if (orderBy == null)
                orderBy = dummyObj;
            SDBMessage rtn = AdminCommand(command, matcher, selector, orderBy, dummyObj);

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

        /** \fn DBCursor GetList(int listType)
         *  \brief Get the informations of specified type
         *  \param listType The specified type as below:
         *  
         *      SDB_LIST_CONTEXTS
         *      SDB_LIST_CONTEXTS_CURRENT
         *      SDB_LIST_SESSIONS
         *      SDB_LIST_SESSIONS_CURRENT
         *      SDB_LIST_COLLECTIONS
         *      SDB_LIST_COLLECTIONSPACES
         *      SDB_LIST_STORAGEUNITS
         *      SDB_LIST_GROUPS
         *      SDB_LIST_STOREPROCEDURES
         *      SDB_LIST_DOMAINS
         *      SDB_LIST_TASKS
         *      SDB_LIST_CS_IN_DOMAIN
         *      SDB_LIST_CL_IN_DOMAIN
         *  \return A DBCursor of all the fitted objects or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetList(int listType)
        {
            BsonDocument dummyObj = new BsonDocument();
            return GetList(listType, dummyObj, dummyObj, dummyObj);
        }

        /** \fn DBCursor GetList(int listType, BsonDocument matcher, BsonDocument selector,
                                          BsonDocument orderBy)
         *  \brief Get the informations of specified type
         *  \param listType The specified type as below:
         *  
         *      SDB_LIST_CONTEXTS
         *      SDB_LIST_CONTEXTS_CURRENT
         *      SDB_LIST_SESSIONS
         *      SDB_LIST_SESSIONS_CURRENT
         *      SDB_LIST_COLLECTIONS
         *      SDB_LIST_COLLECTIONSPACES
         *      SDB_LIST_STORAGEUNITS
         *      SDB_LIST_GROUPS
         *      SDB_LIST_STOREPROCEDURES
         *      SDB_LIST_DOMAINS
         *      SDB_LIST_TASKS
         *      SDB_LIST_CS_IN_DOMAIN
         *      SDB_LIST_CL_IN_DOMAIN
         *      
         *  \param matcher The matching condition or null
         *  \param selector The selective rule or null
         *  \param orderBy The ordered rule or null
         *  \return A DBCursor of all the fitted objects or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor GetList(int listType, BsonDocument matcher, BsonDocument selector,
                                BsonDocument orderBy)
        {
            string command = null;
            switch (listType)
            {
                case SDBConst.SDB_LIST_CONTEXTS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.CONTEXTS;
                    break;
                case SDBConst.SDB_LIST_CONTEXTS_CURRENT:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.CONTEXTS_CUR;
                    break;
                case SDBConst.SDB_LIST_SESSIONS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.SESSIONS;
                    break;
                case SDBConst.SDB_LIST_SESSIONS_CURRENT:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.SESSIONS_CUR;
                    break;
                case SDBConst.SDB_LIST_COLLECTIONS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.COLLECTIONS;
                    break;
                case SDBConst.SDB_LIST_COLLECTIONSPACES:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.COLSPACES;
                    break;
                case SDBConst.SDB_LIST_STORAGEUNITS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.STOREUNITS;
                    break;
                case SDBConst.SDB_LIST_GROUPS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.GROUPS;
                    break;
                case SDBConst.SDB_LIST_STOREPROCEDURES:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.PROCEDURES;
                    break;
                case SDBConst.SDB_LIST_DOMAINS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.DOMAINS;
                    break;
                case SDBConst.SDB_LIST_TASKS:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.TASKS;
                    break;
                case SDBConst.SDB_LIST_CS_IN_DOMAIN:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.CS_IN_DOMAIN;
                    break;
                case SDBConst.SDB_LIST_CL_IN_DOMAIN:
                    command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_CMD + " " +
                           SequoiadbConstants.CL_IN_DOMAIN;
                    break;
                default:
                    throw new BaseException("SDB_INVALIDARG");
            }

            BsonDocument dummyObj = new BsonDocument();
            if (matcher == null)
                matcher = dummyObj;
            if (selector == null)
                selector = dummyObj;
            if (orderBy == null)
                orderBy = dummyObj;
            SDBMessage rtn = AdminCommand(command, matcher, selector, orderBy, dummyObj);

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

        /** \fn void ResetSnapshot( BsonDocument matcher )
         *  \brief Reset the snapshot
         *  \param matcher The matching condition 
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void ResetSnapshot(BsonDocument matcher)
        {
            BsonDocument dummyObj = new BsonDocument();
            if (matcher == null)
                matcher = dummyObj;
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " "
                             + SequoiadbConstants.RESET;
            SDBMessage rtn = AdminCommand(command, matcher, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void BackupOffline(BsonDocument options)
         *  \brief Backup the whole database or specifed replica group.
         *  \param options Contains a series of backup configuration infomations. 
         *         Backup the whole cluster if null. The "options" contains 5 options as below. 
         *         All the elements in options are optional. 
         *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", 
         *             "Name":"backupName", "Description":description, "EnsureInc":true, "OverWrite":true}
         *         <ul>
         *          <li>GroupName   : The replica groups which to be backuped
         *          <li>Path        : The backup path, if not assign, use the backup path assigned in configuration file
         *          <li>Name        : The name for the backup
         *          <li>Description : The description for the backup
         *          <li>EnsureInc   : Whether excute increment synchronization, default to be false
         *          <li>OverWrite   : Whether overwrite the old backup file, default to be false
         *         </ul>
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void BackupOffline(BsonDocument options)
        {
            // check argument
            if (options == null || options.ElementCount == 0)
                throw new BaseException("INVALIDARG");
            foreach (string key in options.Names)
            {
                if (key.Equals(SequoiadbConstants.FIELD_GROUPNAME) ||
                    key.Equals(SequoiadbConstants.FIELD_NAME) ||
                    key.Equals(SequoiadbConstants.FIELD_PATH) ||
                    key.Equals(SequoiadbConstants.FIELD_DESP) ||
                    key.Equals(SequoiadbConstants.FIELD_ENSURE_INC) ||
                    key.Equals(SequoiadbConstants.FIELD_OVERWRITE))
                    continue;
                else
                    throw new BaseException("INVALIDARG");
            }
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.BACKUP_OFFLINE_CMD;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(commandString,
                                          options, dummyObj, dummyObj, dummyObj);
            // check return flag
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn DBCursor ListBackup(BsonDocument options, BsonDocument matcher,
		 *	                        BsonDocument selector, BsonDocument orderBy)
         *  \brief List the backups.
         *  \param options Contains configuration infomations for remove backups, list all the backups in the default backup path if null.
         *         The "options" contains 3 options as below. All the elements in options are optional. 
         *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
         *         <ul>
         *          <li>GroupName   : Assign the backups of specifed replica groups to be list
         *          <li>Path        : Assign the backups in specifed path to be list, if not assign, use the backup path asigned in the configuration file
         *          <li>Name        : Assign the backups with specifed name to be list
         *         </ul>
         *  \param matcher The matching rule, return all the documents if null
         *  \param selector The selective rule, return the whole document if null
         *  \param orderBy The ordered rule, never sort if null
         *  \return the DBCursor of the backup or null while having no backup infonation
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListBackup(BsonDocument options, BsonDocument matcher,
                                   BsonDocument selector, BsonDocument orderBy)
        {
            // check argument
            if (options != null)
            {
                foreach (string key in options.Names)
                {
                    if (key.Equals(SequoiadbConstants.FIELD_GROUPNAME) ||
                        key.Equals(SequoiadbConstants.FIELD_NAME) ||
                        key.Equals(SequoiadbConstants.FIELD_PATH))
                        continue;
                    else
                        throw new BaseException("INVALIDARG");
                }
            }
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_BACKUP_CMD;
            // run command
            SDBMessage rtn = AdminCommand(commandString,
                                          matcher, selector, orderBy, options);
            // check return flag and retrun cursor
            DBCursor cursor = null;
            int flags = rtn.Flags;
            if (flags != 0)
            {
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                    throw new BaseException(flags);
            }
            cursor = new DBCursor(rtn, this);
            return cursor;
        }

        /** \fn void RemoveBackup ( BsonDocument options )
         *  \brief Remove the backups.
         *  \param options Contains configuration infomations for remove backups, remove all the backups in the default backup path if null.
         *                 The "options" contains 3 options as below. All the elements in options are optional.
         *                 eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
         *                 <ul>
         *                  <li>GroupName   : Assign the backups of specifed replica grouops to be remove
         *                  <li>Path        : Assign the backups in specifed path to be remove, if not assign, use the backup path asigned in the configuration file
         *                  <li>Name        : Assign the backups with specifed name to be remove
         *                 </ul>
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void RemoveBackup(BsonDocument options)
        {
            // check argument
            if (options != null)
            {
                foreach (string key in options.Names)
                {
                    if (key.Equals(SequoiadbConstants.FIELD_GROUPNAME) ||
                        key.Equals(SequoiadbConstants.FIELD_NAME) ||
                        key.Equals(SequoiadbConstants.FIELD_PATH))
                        continue;
                    else
                        throw new BaseException("INVALIDARG");
                }
            }
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.REMOVE_BACKUP_CMD;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(commandString,
                                          options, dummyObj, dummyObj, dummyObj);
            // check return flag
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn DBCursor ListTasks(BsonDocument matcher, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
         *  \brief List the tasks.
         *  \param matcher The matching rule, return all the documents if null
         *  \param selector The selective rule, return the whole document if null
         *  \param orderBy The ordered rule, never sort if null
         *  \param hint The hint, automatically match the optimal hint if null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListTasks(BsonDocument matcher, BsonDocument selector, BsonDocument orderBy, BsonDocument hint)
        {
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.LIST_TASK_CMD;
            // run command
            SDBMessage rtn = AdminCommand(commandString,
                                          matcher, selector, orderBy, hint);
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
            // return the result by cursor
            DBCursor cursor = null;
            cursor = new DBCursor(rtn, this);
            return cursor;
        }

        /** \fn void WaitTasks(List<long> taskIDs)
         *  \brief Wait the tasks to finish.
         *  \param taskIDs The list of task id
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void WaitTasks(List<long> taskIDs)
        {
            // check arguments
            if (taskIDs == null || taskIDs.Count == 0)
                throw new BaseException("SDB_INVALIDARG");
            // build bson to send
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument newObj = new BsonDocument();
            BsonDocument subObj = new BsonDocument();
            BsonArray subArr = new BsonArray(taskIDs);
            subObj.Add("$in", subArr);
            newObj.Add(SequoiadbConstants.FIELD_TASKID, subObj);
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.WAIT_TASK_CMD;
            // run command
            SDBMessage rtn = AdminCommand(commandString,
                                          newObj, dummyObj, dummyObj, dummyObj);
            // check return flag
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn void CancelTask(long taskIDs, bool isAsync)
         *  \brief Cancel the specified task.
         *  \param taskID The task id
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void CancelTask(long taskID, bool isAsync)
        {
            // check arguments
            if (taskID <= 0)
                throw new BaseException("SDB_INVALIDARG");
            // build bson to send
            BsonDocument dummyObj = new BsonDocument();
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_TASKID, taskID);
            newObj.Add(SequoiadbConstants.FIELD_ASYNC, isAsync);
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CANCEL_TASK_CMD;
            // run command
            SDBMessage rtn = AdminCommand(commandString,
                                          newObj, dummyObj, dummyObj, dummyObj);
            // check return flag
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn void SetSessionAttr(BsonDocument options)
         *  \brief Set the attributes of the session.
         *  \param options The configuration options for session.The options are as below:
         *  
         *      PreferedInstance : indicate which instance to respond read request in current session.
         *                         eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7},
         *                         prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
         *                         default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query. 
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void SetSessionAttr(BsonDocument options)
        {
            // check argument
            if (options == null || !options.Contains(SequoiadbConstants.FIELD_PREFERED_INSTANCE))
                throw new BaseException("SDB_INVALIDARG");
            // build a bson to send
            BsonDocument attrObj = new BsonDocument();
            BsonValue value = options.GetValue(SequoiadbConstants.FIELD_PREFERED_INSTANCE);
            if (value.IsInt32)
            {
                int v = options[SequoiadbConstants.FIELD_PREFERED_INSTANCE].AsInt32;
                if (v < 1 || v > 7)
                    throw new BaseException("SDB_INVALIDARG");
                attrObj.Add(SequoiadbConstants.FIELD_PREFERED_INSTANCE, v);
            }
            else if (value.IsString)
            {
                string v = options[SequoiadbConstants.FIELD_PREFERED_INSTANCE].AsString;
                int val = (int)PreferInstanceType.INS_TYPE_MIN;
                if (v.Equals("M") || v.Equals("m"))
                    val = (int)PreferInstanceType.INS_MASTER;
                else if (v.Equals("S") || v.Equals("s"))
                    val = (int)PreferInstanceType.INS_SLAVE;
                else if (v.Equals("A") || v.Equals("a"))
                    val = (int)PreferInstanceType.INS_SLAVE;
                else
                    throw new BaseException("SDB_INVALIDARG");
                attrObj.Add(SequoiadbConstants.FIELD_PREFERED_INSTANCE, val);
            }
            else
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // build command
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SETSESS_ATTR;
            // run command
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(commandString, attrObj, dummyObj, dummyObj, dummyObj);
            // check return flag
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn void CloseAllCursors()
         *  \brief Close all the cursors created in current connection, 
         *         we can't use those cursors to get data from db engine again,
         *         but, there are some data cache in local
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void CloseAllCursors()
        {
            // TODO: it's better for us to use DBCursor::Close() to close all the cursor
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_KILL_ALL_CONTEXTS;
            byte[] request = SDBMessageHelper.BuildKillAllContextsRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
        }

        /** \fn bool IsDomainExist(string dmName)
         *  \brief Verify the existence of domain in current database
         *  \param dmName The domain name
         *  \return True if collection existed or False if not existed
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public bool IsDomainExist(string dmName)
        {
            if (null == dmName || dmName.Equals(""))
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            BsonDocument matcher = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_NAME, dmName);
            DBCursor cursor = GetList(SDBConst.SDB_LIST_DOMAINS, matcher, null, null);
            if (null != cursor && null != cursor.Next())
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /** \fn Domain CreateDomain(string domainName, BsonDocument options)
         *  \brief Create a domain.
         *  \param domainName The name of the domain
         *  \param options The options for the domain. The options are as below:
         *  
         *      Group:     the list of replica groups' names which the domain is going to contain.
         *                 eg: { "Group": [ "group1", "group2", "group3" ] }
         *                 If this argument is not included, the domain will contain all replica groups in the cluster.
         *      AutoSplit: If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
         *                 the data of this collection will be split(hash split) into all the groups in this domain automatically.
         *                 However, it won't automatically split data into those groups which were add into this domain later.
         *                 eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
         *  \return The created Domain instance
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public Domain CreateDomain(string domainName, BsonDocument options)
        {
            // check
            if (null == domainName || domainName.Equals(""))
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            if (IsDomainExist(domainName))
            {
                throw new BaseException("SDB_CAT_DOMAIN_EXIST");
            }
            // build object
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_NAME, domainName);
            if (null != options)
            {
                newObj.Add(SequoiadbConstants.FIELD_OPTIONS, options);
            }
            // build cmd
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_CMD
                             + " " + SequoiadbConstants.DOMAIN;
            // run command
            SDBMessage rtn = AdminCommand(command, newObj, null, null, null);
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
            return new Domain(this, domainName);
        }

        /** \fn void DropDomain(string domainName)
         *  \brief Drop a domain.
         *  \param domainName The name of the domain
         *  \return The created Domain instance
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void DropDomain(string domainName)
        {
            // check
            if (null == domainName || domainName.Equals(""))
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            // build object
            BsonDocument newObj = new BsonDocument();
            newObj.Add(SequoiadbConstants.FIELD_NAME, domainName);
            // build cmd
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.DROP_CMD
                             + " " + SequoiadbConstants.DOMAIN;
            // run command
            SDBMessage rtn = AdminCommand(command, newObj, null, null, null);
            int flags = rtn.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
        }

        /** \fn Domain GetDomain(string domainName)
         *  \brief Get the specified domain.
         *  \param domainName The name of the domain
         *  \return The created Domain instance
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public Domain GetDomain(string domainName)
        {
            if (IsDomainExist(domainName))
            {
                return new Domain(this, domainName);
            }
            else
            {
                throw new BaseException("SDB_CAT_DOMAIN_NOT_EXIST");
            }
        }

        /** \fn DBCursor ListDomains(BsonDocument matcher, BsonDocument selector,
         *                           BsonDocument orderBy)
         *  \brief List domains.
         *  \param matcher The matching rule, return all the documents if null
         *  \param selector The selective rule, return the whole document if null
         *  \param orderBy The ordered rule, never sort if null
         *  \return the cursor of the result.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListDomains(BsonDocument matcher, BsonDocument selector,
                                    BsonDocument orderBy, BsonDocument hint)
        {
            return GetList(SDBConst.SDB_LIST_DOMAINS, matcher, selector, orderBy);
        }

        /** \fn DBCursor ListReplicaGroups()
         *  \brief Get all the groups
         *  \return A cursor of all the groups
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public DBCursor ListReplicaGroups()
        {
            BsonDocument dummyObj = new BsonDocument();
            return GetList(SDBConst.SDB_LIST_GROUPS, dummyObj, dummyObj, dummyObj);
        }

        /** \fn ReplicaGroup GetReplicaGroup(string groupName)
         *  \brief Get the ReplicaGroup by name
         *  \param groupName The group name
         *  \return The fitted ReplicaGroup or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public ReplicaGroup GetReplicaGroup(string groupName)
        {
            BsonDocument matcher = new BsonDocument();
            BsonDocument dummyobj = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_GROUPNAME, groupName);
            DBCursor cursor = GetList(SDBConst.SDB_LIST_GROUPS, matcher, dummyobj, dummyobj);
            if (cursor != null)
            {
                BsonDocument detail = cursor.Next();
                if (detail != null)
                {
                    try
                    {
                        if (!detail[SequoiadbConstants.FIELD_GROUPID].IsInt32)
                            throw new BaseException("SDB_SYS");
                        int groupID = detail[SequoiadbConstants.FIELD_GROUPID].AsInt32;
                        return new ReplicaGroup(this, groupName, groupID);
                    }
                    catch (KeyNotFoundException)
                    {
                        throw new BaseException("SDB_SYS");
                    }
                }
                else
                    return null;
            }
            else
                throw new BaseException("SDB_SYS");
        }

        /** \fn ReplicaGroup GetReplicaGroup(int groupID)
         *  \brief Get the ReplicaGroup by ID
         *  \param groupID The group ID
         *  \return The fitted ReplicaGroup or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public ReplicaGroup GetReplicaGroup(int groupID)
        {
            BsonDocument matcher = new BsonDocument();
            BsonDocument dummyobj = new BsonDocument();
            matcher.Add(SequoiadbConstants.FIELD_GROUPID, groupID);
            DBCursor cursor = GetList(SDBConst.SDB_LIST_GROUPS, matcher, dummyobj, dummyobj);
            if (cursor != null)
            {
                BsonDocument detail = cursor.Next();
                if (detail != null)
                    try
                    {
                        if (!detail[SequoiadbConstants.FIELD_GROUPNAME].IsString)
                            throw new BaseException("SDB_SYS");
                        string groupName = detail[SequoiadbConstants.FIELD_GROUPNAME].AsString;
                        return new ReplicaGroup(this, groupName, groupID);
                    }
                    catch (KeyNotFoundException)
                    {
                        throw new BaseException("SDB_SYS");
                    }
                else
                    return null;
            }
            else
                throw new BaseException("SDB_SYS");
        }

        /** \fn ReplicaGroup CreateReplicaGroup(string groupName)
         *  \brief Create the ReplicaGroup with given name
         *  \param groupName The group name
         *  \return The ReplicaGroup has been created succefully
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public ReplicaGroup CreateReplicaGroup(string groupName)
        {
            if (groupName == null)
                throw new BaseException("SDB_INVALIDARG");
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_CMD + " "
                             + SequoiadbConstants.GROUP;
            BsonDocument condition = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_GROUPNAME, groupName);
            BsonDocument dummyObj = new BsonDocument();

            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
            else
                return GetReplicaGroup(groupName);
        }

        /** \fn ReplicaGroup RemoveReplicaGroup(string groupName)
         *  \brief Remove the ReplicaGroup with given name
         *  \param groupName The group name
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         *  \note We can't remove a replica group which has data
         */
        public void RemoveReplicaGroup(string groupName)
        {
            if (groupName == null)
                throw new BaseException("SDB_INVALIDARG");
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.REMOVE_CMD + " "
                             + SequoiadbConstants.GROUP;
            BsonDocument condition = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_GROUPNAME, groupName);
            BsonDocument dummyObj = new BsonDocument();

            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }
        /** \fn void CreateReplicaCataGroup(string hostName, int port, string dbpath,
                                            BsonDocument configure) 
         *  \brief Create the Replica Catalog Group with given options
         *  \param hostName The host name
         *  \param port The port
         *  \param dbpath The database path
         *  \param configure The configure options
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void CreateReplicaCataGroup(string hostName, int port, string dbpath,
                                            BsonDocument configure)
        {
            if (hostName == null || port == 0 || dbpath == null)
                throw new BaseException("SDB_INVALIDARG");
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_CMD + " "
                             + SequoiadbConstants.CATALOG + " " + SequoiadbConstants.GROUP;
            BsonDocument condition = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_HOSTNAME, hostName);
            condition.Add(SequoiadbConstants.SVCNAME, port.ToString());
            condition.Add(SequoiadbConstants.DBPATH, dbpath);
            if (configure != null)
            {
                IEnumerator<BsonElement> it = configure.GetEnumerator();
                while (it.MoveNext())
                {
                    BsonElement e = it.Current;
                    if (e.Name == SequoiadbConstants.FIELD_HOSTNAME ||
                         e.Name == SequoiadbConstants.SVCNAME ||
                         e.Name == SequoiadbConstants.DBPATH)
                        continue;
                    condition.Add(e.Name, e.Value.AsString);
                }
            }

            BsonDocument dummyObj = new BsonDocument();

            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }

        /** \fn ReplicaGroup ActivateReplicaGroup(string groupName)
         *  \brief Activate the ReplicaGroup with given name
         *  \param groupName The group name
         *  \return The ReplicaGroup has been activated if succeed or null if fail
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public ReplicaGroup ActivateReplicaGroup(string groupName)
        {
            ReplicaGroup group = GetReplicaGroup(groupName);
            bool result = group.Start();
            if (result)
                return group;
            else
                return null;
        }

        private SDBMessage CreateCS(string csName, BsonDocument options)
        {
            string commandString = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.CREATE_CMD + " " + SequoiadbConstants.COLSPACE;
            BsonDocument cObj = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage sdbMessage = new SDBMessage();

            cObj.Add(SequoiadbConstants.FIELD_NAME, csName);
            cObj.Add(options);
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.Matcher = cObj;
            sdbMessage.CollectionFullName = commandString;

            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
            sdbMessage.Flags = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.SkipRowsCount = 0;
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

        private SDBMessage AdminCommand(string cmdType, string contextType, string contextName)
        {
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage sdbMessage = new SDBMessage();
            string commandString = SequoiadbConstants.ADMIN_PROMPT + cmdType + " " + contextType;
            if (!(cmdType.Equals(SequoiadbConstants.LIST_CMD) || cmdType.Equals(SequoiadbConstants.SNAP_CMD)))
            {
                BsonDocument cObj = new BsonDocument();
                cObj.Add(SequoiadbConstants.FIELD_NAME, contextName);
                sdbMessage.Matcher = cObj;
            }
            else
                sdbMessage.Matcher = dummyObj;
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.CollectionFullName = commandString;

            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = 0;
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
            if (connection == null)
                throw new BaseException("SDB_NETWORK");
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
                {
                    if (flags == SequoiadbConstants.SDB_DMS_EOC)
                        hasMore = false;
                    else
                    {
                        throw new BaseException(flags);
                    }
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

        private bool RequestSysInfo()
        {
            byte[] request = SDBMessageHelper.BuildSysInfoRequest();
            connection.SendMessage(request);
            int osType = 0;
            return SDBMessageHelper.ExtractSysInfoReply(connection.ReceiveMessage(128), ref osType);
        }

        private void Auth()
        {
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.MSG_AUTH_VERIFY_REQ;
            sdbMessage.RequestID = 0;
            MD5 md5 = MD5.Create();
            byte[] data = md5.ComputeHash(Encoding.Default.GetBytes(password));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
                builder.Append(data[i].ToString("x2"));
            byte[] request = SDBMessageHelper.BuildAuthMsg(sdbMessage, userName, builder.ToString(), isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);
        }
    }
}
