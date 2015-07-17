using System;
using SequoiaDB.Bson;
using System.Net;
using System.Collections.Generic;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class Node
     *  \brief Database operation interfaces of node.
     */
    public class Node
    {
        private int nodeID = -1;
        private string nodeName;
        private string hostName;
        private int port;
        internal bool isBigEndian = false;
        private ReplicaGroup group;

        internal Node(ReplicaGroup group, string hostName, int port, int nodeID)
        {
            this.group = group;
            this.hostName = hostName;
            this.port = port;
            this.nodeName = hostName + SequoiadbConstants.NODE_NAME_SERVICE_SEP + port;
            this.nodeID = nodeID;
            this.isBigEndian = group.isBigEndian;
        }

        /** \property ReplicaGroup
         *  \brief Return the replica group instance of current node 
         *  \return The ReplicaGroup object
         */
        public ReplicaGroup ReplicaGroup
        {
            get { return group; }
        }

        /** \property NodeName
         *  \brief Return the name of current node
         *  \return The node name
         */
        public string NodeName
        {
            get { return nodeName; }
        }

        /** \property HostName
         *  \brief Return the host name of current node
         *  \return The host name
         */
        public string HostName
        {
            get { return hostName; }
        }

        /** \property Port
         *  \brief Return the port of current node
         *  \return The port
         */
        public int Port
        {
            get { return port; }
        }

        /** \property NodeID
         *  \brief Return the node ID of current node
         *  \return The node ID
         */
        public int NodeID
        {
            get { return nodeID; }
        }

        /** \fn bool Stop()
         *  \brief Stop the current node
         *  \return True if succeed or False if fail
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public bool Stop()
        {
            bool start = false;
            return StopStart(start);
        }

        /** \fn bool Start()
         *  \brief Start the current node
         *  \return True if succeed or False if fail
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public bool Start()
        {
            bool start = true;
            return StopStart(start);
        }

        /** \fn SDBConst.NodeStatus GetStatus()
         *  \brief Get the status of current node
         *  \return The status of current node
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public SDBConst.NodeStatus GetStatus()
        {
            SDBConst.NodeStatus status = SDBConst.NodeStatus.SDB_NODE_UNKNOWN;
            string command = SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SNAP_CMD + " "
                             + SequoiadbConstants.DATABASE;
            BsonDocument condition = new BsonDocument();
            BsonDocument dummyObj = new BsonDocument();
            condition.Add(SequoiadbConstants.FIELD_GROUPID, group.GroupID);
            condition.Add(SequoiadbConstants.FIELD_NODEID, nodeID);
            SDBMessage rtn = AdminCommand(command, condition, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags == 0)
                status = SDBConst.NodeStatus.SDB_NODE_ACTIVE;
            else if (flags == (int)Errors.errors.SDB_NET_CANNOT_CONNECT)
                status = SDBConst.NodeStatus.SDB_NODE_INACTIVE;

            return status;
        }

        /** \fn Sequoiadb Connect()
         *  \brief Connect to remote Sequoiadb database node
         *  \return The Sequoiadb handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public Sequoiadb Connect()
        {
            return Connect("", "");
        }

        /** \fn Sequoiadb Connect(string username, string password)
         *  \brief Connect to remote Sequoiadb database node
         *  \username Sequoiadb connection user name
         *  \password Sequoiadb connection password
         *  \return The Sequoiadb handle
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public Sequoiadb Connect(string username, string password)
        {
            Sequoiadb sdb = null;
            sdb = new Sequoiadb(hostName, port);
            sdb.Connect(username, password);
            return sdb;
        }

        private bool StopStart(bool start)
        {
            string command = start ? SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.STARTUP_CMD + " "
                                    + SequoiadbConstants.NODE :
                                   SequoiadbConstants.ADMIN_PROMPT + SequoiadbConstants.SHUTDOWN_CMD + " "
                                    + SequoiadbConstants.NODE;
            BsonDocument configuration = new BsonDocument();
            configuration.Add(SequoiadbConstants.FIELD_HOSTNAME, hostName);
            configuration.Add(SequoiadbConstants.SVCNAME, port.ToString());
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage rtn = AdminCommand(command, configuration, dummyObj, dummyObj, dummyObj);
            int flags = rtn.Flags;
            if (flags != 0)
                return false;
            else
                return true;
        }

        private SDBMessage AdminCommand(string command, BsonDocument arg1, BsonDocument arg2,
                                        BsonDocument arg3, BsonDocument arg4)
        {
            IConnection connection = group.SequoiaDB.Connection;
            BsonDocument dummyObj = new BsonDocument();
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_QUERY;
            sdbMessage.Matcher = arg1;
            sdbMessage.Selector = arg2;
            sdbMessage.OrderBy = arg3;
            sdbMessage.Hint = arg4;
            // arg1
            if (null == arg1)
            {
                sdbMessage.Matcher = dummyObj;
            }
            else
            {
                sdbMessage.Matcher = arg1;
            }
            // arg2
            if (null == arg2)
            {
                sdbMessage.Selector = dummyObj;
            }
            else
            {
                sdbMessage.Selector = arg2;
            }
            // arg3
            if (null == arg3)
            {
                sdbMessage.OrderBy = dummyObj;
            }
            else
            {
                sdbMessage.OrderBy = arg3;
            }
            // arg4
            if (null == arg4)
            {
                sdbMessage.Hint = dummyObj;
            }
            else
            {
                sdbMessage.Hint = arg4;
            }
            sdbMessage.CollectionFullName = command;
            sdbMessage.Flags = 0;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            sdbMessage.SkipRowsCount = -1;
            sdbMessage.ReturnRowsCount = -1;

            byte[] request = SDBMessageHelper.BuildQueryRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            return rtnSDBMessage;
        }
    }
}
