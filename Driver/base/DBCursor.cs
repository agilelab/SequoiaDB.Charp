using System;
using System.Collections.Generic;
using SequoiaDB.Bson;
using System.Collections;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class DBCursor
     *  \brief Database collection cursor
     */
    public class DBCursor : IEnumerable
    {
        private ulong reqId = 0;
        private long contextId = -1;
        private BsonDocument hint = null;
        private SDBMessage sdbMessage = null;
        private DBCollection dbc = null;
        private IConnection connection = null;
        private List<BsonDocument> list = null;
        private int index = -1;
        private bool hasMore = false;
        private bool isBigEndian = false;
        private bool isClosed = false;

        internal DBCursor(SDBMessage rtnSDBMessage, DBCollection dbc)
        {
            this.dbc = dbc;
            connection = dbc.CollSpace.SequoiaDB.Connection;
            hint = new BsonDocument();
            hint.Add("", SequoiadbConstants.CLIENT_RECORD_ID_INDEX);
            sdbMessage = new SDBMessage();
            reqId = rtnSDBMessage.RequestID;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.ContextIDList = rtnSDBMessage.ContextIDList;
            contextId = sdbMessage.ContextIDList[0];
            sdbMessage.NumReturned = -1;    // return data count
            list = rtnSDBMessage.ObjectList; // while using fineOne, ObjectList may have data
            hasMore = true;
            isBigEndian = dbc.isBigEndian;
            isClosed = false;
            this.DBQuery = new DBQuery();
        }

        internal DBCursor(SDBMessage rtnSDBMessage, Sequoiadb sdb )
        {
            this.connection = sdb.Connection;
            hint = new BsonDocument();
            hint.Add("", SequoiadbConstants.CLIENT_RECORD_ID_INDEX);
            sdbMessage = new SDBMessage();
            reqId = rtnSDBMessage.RequestID;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.ContextIDList = rtnSDBMessage.ContextIDList;
            contextId = sdbMessage.ContextIDList[0];
            sdbMessage.NumReturned = -1;    // return data count
            list = rtnSDBMessage.ObjectList; // while using fineOne, ObjectList may have data
            hasMore = true;
            isBigEndian = sdb.isBigEndian;
            isClosed = false;
            this.DBQuery = new DBQuery();
        }

        ~DBCursor()
        {
        }

        /** \fn BsonDocument Next()
         *  \brief Get the next Bson of this cursor
         *  \return BsonDocument or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public BsonDocument Next()
        {
            if ( isClosed )
            {
                throw new BaseException("SDB_DMS_CONTEXT_IS_CLOSE");
            }
            // when using findOne, list may contain data while index is -1
            if ((index == -1) && hasMore && (list == null))
            {
                ReadNextBuffer();
            }
            if (list == null)
            {
                return null;
            }
            if (index < list.Count - 1)
            {
                return list[++index];
            }
            else
            {
                index = -1;
                list = null;
                return Next();
            }
        }

        /** \fn BsonDocument Current()
         *  \brief Get the current Bson of this cursor
         *  \return BsonDocument or null
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public BsonDocument Current()
        {
            if ( isClosed )
            {
                throw new BaseException("SDB_DMS_CONTEXT_IS_CLOSE");
            }
            if ( index == -1 )
                return Next();
            else
                return list[index];
        }

        /** \fn void Close()
         *  \brief Close current cursor
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Close()
        {
            if (isClosed)
            {
                return;
            }
            KillCursor();
            isClosed = true;
        }

        /* \fn void UpdateCurrent(BsonDocument modifier)
        *  \brief Update the current Bson of this cursor
        *  \param modifier The updating rule
        *  \exception SequoiaDB.BaseException
        *  \exception System.Exception
        */

        /*
        public void UpdateCurrent(BsonDocument modifier)
        {
            if (modifier == null)
                throw new BaseException("SDB_INVALIDARG");
            if (dbc == null)
                throw new BaseException("SDB_CLT_OBJ_NOT_EXIST");
            BsonDocument current;
            if (( current = Current()) != null )
            {
                BsonDocument matcher = new BsonDocument();
                matcher.Add(SequoiadbConstants.OID, current[SequoiadbConstants.OID].AsObjectId);
                dbc.Update(matcher, modifier, hint);
                BsonDocument dummy = new BsonDocument();
                list[index] = dbc.Query(matcher, dummy, dummy, dummy).Next();
            }
        }
        */

        /* \fn void DeleteCurrent()
        *  \brief Delete the current Bson of this cursor
        *  \exception SequoiaDB.BaseException
        *  \exception System.Exception
        */
        /*
        public void DeleteCurrent()
        {
            if ( Current() != null )
                dbc.Delete( list[index] );
            list.RemoveAt(index);
            if (index >= list.Count)
                index = -1;
        }
        */

        private void ReadNextBuffer()
        {
            if (connection == null)
            {
                throw new BaseException("SDB_NOT_CONNECTED");
            }
            if (-1 == contextId)
            {
                hasMore = false;
                index = -1;
                dbc = null;
                list = null;
                return;
            }

            sdbMessage.OperationCode = Operation.OP_GETMORE;
            sdbMessage.RequestID = reqId;
            byte[] request = SDBMessageHelper.BuildGetMoreRequest(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            
            int flags = rtnSDBMessage.Flags;
            if (flags == SequoiadbConstants.SDB_DMS_EOC)
            {
                hasMore = false;
                index = -1 ;
                dbc = null ;
                list = null;
            }
            else if (flags != 0)
            {
                throw new BaseException(flags);
            }
            else
            {
                reqId = rtnSDBMessage.RequestID;
                list = rtnSDBMessage.ObjectList;
            }
        }

        internal void KillCursor()
        {
            if ((connection == null) || 
                (connection.IsClosed()) || 
                (contextId == -1))
            {
                return;
            }
            SDBMessage sdbMessage = new SDBMessage();
            sdbMessage.OperationCode = Operation.OP_KILL_CONTEXT;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(contextId);
            byte[] request = SDBMessageHelper.BuildKillCursorMsg(sdbMessage, isBigEndian);
            connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                throw new BaseException(flags);

            connection = null;
            contextId = -1;
        }

        /// <summary>
        /// Gets the non-generic enumerator.
        /// </summary>
        /// <returns>The enumerator.</returns>
        protected virtual IEnumerator IEnumerableGetEnumerator()
        {
            return null;
        }

        /// <summary>
        /// Returns the size of the result set (honors Skip and Limit, unlike Count which does not).
        /// </summary>
        /// <returns>The size of the result set.</returns>
        public virtual long Size()
        {
            return this.dbc.GetCount(DBQuery.Matcher);
        }

        #region IEnumerable 成员

        IEnumerator IEnumerable.GetEnumerator()
        {
            return IEnumerableGetEnumerator();
        }

        #endregion

        internal DBQuery DBQuery
        {
            get;
            set;
        }
    }
}