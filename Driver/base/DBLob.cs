using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SequoiaDB.Bson;

namespace SequoiaDB.Driver
{

    /** \class DBLob
     *  \brief Database operation interfaces of large object
     */
	public class  DBLob
	{
        /**
         *  \memberof SDB_LOB_SEEK_SET 0
         *  \brief Change the position from the beginning of lob 
         */
        public const int SDB_LOB_SEEK_SET   = 0;
    
        /**
         *  \memberof SDB_LOB_SEEK_CUR 1
         *  \brief Change the position from the current position of lob 
         */
        public const int SDB_LOB_SEEK_CUR   = 1;
    
        /**
         *  \memberof SDB_LOB_SEEK_END 2
         *  \brief Change the position from the end of lob 
         */
        public const int SDB_LOB_SEEK_END   = 2;

        /**
         *  \memberof SDB_LOB_CREATEONLY 0x00000001
         *  \brief Open a new lob only
         */
        public const int SDB_LOB_CREATEONLY = 0x00000001;

        /**
         *  \memberof SDB_LOB_READ 0x00000004
         *  \brief Open an existing lob to read
         */
        public const int SDB_LOB_READ       = 0x00000004;

        // the max lob data size to send for one message
        private const int SDB_LOB_MAX_DATA_LENGTH  = 1024 * 1024;
    
        private const long SDB_LOB_DEFAULT_OFFSET  = -1;
        private const int SDB_LOB_DEFAULT_SEQ      = 0;
    
        private DBCollection _cl = null;
        private IConnection  _connection = null;
        internal bool        _isBigEndian = false;

        private ObjectId     _id;
        private int          _mode;
        private long         _size;
        private long         _createTime;
        private long         _readOffset;
        private bool         _isOpen = false;
    
        // when first open/create DBLob, sequoiadb return the contextID for the
        // further reading/writing/close
        private long _contextID ;

        internal DBLob(DBCollection cl)
        {
            this._cl = cl;
            this._connection = cl.CollSpace.SequoiaDB.Connection;
            this._isBigEndian = cl.isBigEndian;
            _id = ObjectId.Empty;
            _mode = -1;
            _size = 0;
            _readOffset = -1;
            _createTime = 0;
            _isOpen = false;
            _contextID = -1;
        }
/*
        internal void Open()
        {
            Open(ObjectId.Empty, SDB_LOB_CREATEONLY);
        }

        internal void Open(ObjectId id)
        {
            Open(id, SDB_LOB_READ);
        }
*/
        /** \fn         Open( ObjectId id, int mode )
         * \brief       Open an exist lob, or create a lob
         * \param       id   the lob's id
         * \param       mode available mode is SDB_LOB_CREATEONLY or SDB_LOB_READ.
         *              SDB_LOB_CREATEONLY 
         *                  create a new lob with given id, if id is null, it will 
         *                  be generated in this function;
         *              SDB_LOB_READ
         *                  read an exist lob
         * \exception SequoiaDB.BaseException
         * \exception System.Exception
         */
        internal void Open(ObjectId id, int mode)
        {
            // check
            if (_isOpen)
            {
                throw new BaseException("SDB_LOB_HAS_OPEN");
            }
            if (SDB_LOB_CREATEONLY != mode && SDB_LOB_READ != mode)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            if (SDB_LOB_READ == mode)
            {
                if (ObjectId.Empty == id)
                {
                    throw new BaseException("SDB_INVALIDARG");
                }
            }
            // gen oid
            _id = id;
            if (SDB_LOB_CREATEONLY == mode)
            {
                if (ObjectId.Empty == _id)
                {
                    _id = ObjectId.GenerateNewId();
                }
            }
            // mode
            _mode = mode;
            _readOffset = 0;
            // open
            _Open();
            _isOpen = true;
        }

        /** \fn          Close()
          * \brief       Close the lob
          * \return void
          * \exception SequoiaDB.BaseException
          * \exception System.Exception
          */
        public void Close()
        {
            /*
                /// close reg msg is |MsgOpLob|
                struct _MsgHeader
                {
                   SINT32 messageLength ; // total message size, including this
                   SINT32 opCode ;        // operation code
                   UINT32 TID ;           // client thead id
                   MsgRouteID routeID ;   // route id 8 bytes
                   UINT64 requestID ;     // identifier for this message
                } ;

                typedef struct _MsgOpLob
                {
                   MsgHeader header ;
                   INT32 version ;
                   SINT16 w ;
                   SINT16 padding ;
                   SINT32 flags ;
                   SINT64 contextID ;
                   UINT32 bsonLen ;
                } MsgOpLob ;
             */
            SDBMessage sdbMessage = new SDBMessage();
            // build sdbMessage
            // MsgHeader
            sdbMessage.OperationCode = Operation.MSG_BS_LOB_CLOSE_REQ;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            // the rest part of _MsgOpLOb
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = (short)0;
            sdbMessage.Flags = SequoiadbConstants.DEFAULT_FLAGS;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(_contextID);
            sdbMessage.BsonLen = 0;

            // build send msg
            byte[] request = SDBMessageHelper.BuildCloseLobRequest(sdbMessage, _isBigEndian);
            // send msg
            _connection.SendMessage(request);
            // receive and extract return msg from engine
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(_connection.ReceiveMessage(_isBigEndian), _isBigEndian);
            // check the result
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
            _isOpen = false;
        }

        /** \fn          Read( byte[] b )
         *  \brief       Reads up to b.length bytes of data from this 
         *               lob into an array of bytes. 
         *  \param       b   the buffer into which the data is read.
         *  \return      the total number of bytes read into the buffer, or
         *               <code>-1</code> if there is no more data because the end of
         *               the file has been reached, or <code>0<code> if 
         *               <code>b.length</code> is Zero.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public int Read(byte[] b)
        {
            if (!_isOpen)
            {
                throw new BaseException("SDB_LOB_NOT_OPEN");
            }
            if (null == b)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
            if (0 == b.Length)
            {
                return 0;
            }
            return _Read(b);
        }

        /** \fn          Write( byte[] b )
         *  \brief       Writes b.length bytes from the specified 
         *               byte array to this lob. 
         *  \param       b   the data.
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Write(byte[] b)
        {
            /*
                /// write req msg is |MsgOpLob|_MsgLobTuple|data|
                struct _MsgHeader
                {
                   SINT32 messageLength ; // total message size, including this
                   SINT32 opCode ;        // operation code
                   UINT32 TID ;           // client thead id
                   MsgRouteID routeID ;   // route id 8 bytes
                   UINT64 requestID ;     // identifier for this message
                } ;

                typedef struct _MsgOpLob
                {
                   MsgHeader header ;
                   INT32 version ;
                   SINT16 w ;
                   SINT16 padding ;
                   SINT32 flags ;
                   SINT64 contextID ;
                   UINT32 bsonLen ;
                } MsgOpLob ;

                union _MsgLobTuple
                {
                   struct
                   {
                      UINT32 len ;
                      UINT32 sequence ;
                      SINT64 offset ;
                   } columns ;

                   CHAR data[16] ;
                } ;
             */
            SDBMessage sdbMessage = new SDBMessage();
            // MsgHeader
            sdbMessage.OperationCode = Operation.MSG_BS_LOB_WRITE_REQ;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            // the rest part of _MsgOpLOb
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = (short)0;
            sdbMessage.Flags = SequoiadbConstants.DEFAULT_FLAGS;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(_contextID);
            sdbMessage.BsonLen = 0;
            // MsgLobTuple
            sdbMessage.LobLen = (uint)b.Length;
            sdbMessage.LobSequence = SDB_LOB_DEFAULT_SEQ;
            sdbMessage.LobOffset = SDB_LOB_DEFAULT_OFFSET;

            // build send msg
            byte[] request = SDBMessageHelper.BuildWriteLobRequest(sdbMessage, b, _isBigEndian);
            // send msg
            _connection.SendMessage(request);

            // receive and extract return msg from engine
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(_connection.ReceiveMessage(_isBigEndian), _isBigEndian);

            // check the result
            int flags = rtnSDBMessage.Flags;
            if (0 != flags)
            {
                throw new BaseException(flags);
            }
            // keep to total number for query
            _size += b.Length;
        }

        /** \fn          void Seek( long size, int seekType )
         *  \brief       Change the read position of the lob. The new position is 
         *               obtained by adding <code>size</code> to the position 
         *               specified by <code>seekType</code>. If <code>seekType</code> 
         *               is set to SDB_LOB_SEEK_SET, SDB_LOB_SEEK_CUR, or SDB_LOB_SEEK_END, 
         *               the offset is relative to the start of the lob, the current 
         *               position of lob, or the end of lob.
         *  \param       size the adding size.
         *  \param       seekType  SDB_LOB_SEEK_SET/SDB_LOB_SEEK_CUR/SDB_LOB_SEEK_END
         *  \return void
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public void Seek(long size, int seekType)
        {
            if (!_isOpen)
            {
                throw new BaseException("SDB_LOB_NOT_OPEN");
            }

            if (_mode != SDB_LOB_READ)
            {
                throw new BaseException("SDB_INVALIDARG");
            }
        
            if ( SDB_LOB_SEEK_SET == seekType )
            {
                if ( size < 0 || size > _size )
                {
                    throw new BaseException( "SDB_INVALIDARG");
                }
                _readOffset = size;
            }
            else if ( SDB_LOB_SEEK_CUR == seekType )
            {
                if ( ( _size < _readOffset + size ) || ( _readOffset + size < 0 ) )
                {
                    throw new BaseException("SDB_INVALIDARG");
                }
                _readOffset += size;
            }
            else if ( SDB_LOB_SEEK_END == seekType )
            {
                if ( size < 0 || size > _size )
                {
                    throw new BaseException( "SDB_INVALIDARG");
                }
                _readOffset = _size - size;
            }
            else
            {
                throw new BaseException("SDB_INVALIDARG");
            }
        }

        /** \fn          bool IsClosed()
         *  \brief       Test whether lob has been closed or not
         *  \return      true for lob has been closed, false for not
         */
        public bool IsClosed()
        {
            return !_isOpen;
        }

        /** \fn          ObjectId GetID()
         *  \brief       Get the lob's id
         *  \return      the lob's id
         */
        public ObjectId GetID()
        {
            return _id;
        }

        /** \fn          long GetSize()
         *  \brief       Get the size of lob
         *  \return      the lob's size
         */
        public long GetSize()
        {
            return _size;
        }

        /** \fn          long GetCreateTime()
         *  \brief       get the create time of lob
         *  \return The lob's create time
         *  \exception SequoiaDB.BaseException
         *  \exception System.Exception
         */
        public long GetCreateTime()
        { 
            return _createTime;
        }

 

        private void _Open()
        {
            /*
                /// open reg msg is |MsgOpLob|bsonobj|
                struct _MsgHeader
                {
                   SINT32 messageLength ; // total message size, including this
                   SINT32 opCode ;        // operation code
                   UINT32 TID ;           // client thead id
                   MsgRouteID routeID ;   // route id 8 bytes
                   UINT64 requestID ;     // identifier for this message
                } ;

                typedef struct _MsgOpLob
                {
                   MsgHeader header ;
                   INT32 version ;
                   SINT16 w ;
                   SINT16 padding ;
                   SINT32 flags ;
                   SINT64 contextID ;
                   UINT32 bsonLen ;
                } MsgOpLob ;
             */
            // add info into object
            BsonDocument openLob = new BsonDocument();
            openLob.Add(SequoiadbConstants.FIELD_COLLECTION, _cl.FullName);
            openLob.Add(SequoiadbConstants.FIELD_LOB_OID, _id);
            openLob.Add(SequoiadbConstants.FIELD_LOB_OPEN_MODE, _mode);

            SDBMessage sdbMessage = new SDBMessage();
            // build sdbMessage
            // MsgHeader
            sdbMessage.OperationCode = Operation.MSG_BS_LOB_OPEN_REQ;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            // the rest part of _MsgOpLOb
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = (short)0;
            sdbMessage.Flags = SequoiadbConstants.DEFAULT_FLAGS;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(SequoiadbConstants.DEFAULT_CONTEXTID);
            sdbMessage.Matcher = openLob;
            // build send msg
            byte[] request = SDBMessageHelper.BuildOpenLobRequest( sdbMessage, _isBigEndian);
            // send msg
            _connection.SendMessage(request);
            // receive and extract return msg from engine
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(_connection.ReceiveMessage(_isBigEndian), _isBigEndian);
            // check the result
            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
            {
                throw new BaseException(flags);
            }
            // get lob info return from engine
            List<BsonDocument> objList = rtnSDBMessage.ObjectList;
            BsonDocument obj = objList[0];
            if (null == obj)
            {
                throw new BaseException("SDB_SYS");
            }
            // lob size
            if (obj.Contains(SequoiadbConstants.FIELD_LOB_SIZE) && obj[SequoiadbConstants.FIELD_LOB_SIZE].IsInt64)
            {
                _size = obj[SequoiadbConstants.FIELD_LOB_SIZE].AsInt64;
            }
            else
            {
                throw new BaseException("SDB_SYS");
            }
            // lob create time
            if (obj.Contains(SequoiadbConstants.FIELD_LOB_CREATTIME) && obj[SequoiadbConstants.FIELD_LOB_CREATTIME].IsInt64)
            {
                _createTime = obj[SequoiadbConstants.FIELD_LOB_CREATTIME].AsInt64;
            }
            else
            {
                throw new BaseException("SDB_SYS");
            }
            // contextID
            _contextID = rtnSDBMessage.ContextIDList[0];
        }

        private int _Read(byte[] b)
        {
            SDBMessage sdbMessage = new SDBMessage();
            // MsgHeader
            sdbMessage.OperationCode = Operation.MSG_BS_LOB_READ_REQ;
            sdbMessage.NodeID = SequoiadbConstants.ZERO_NODEID;
            sdbMessage.RequestID = 0;
            // the rest part of _MsgOpLOb
            sdbMessage.Version = SequoiadbConstants.DEFAULT_VERSION;
            sdbMessage.W = SequoiadbConstants.DEFAULT_W;
            sdbMessage.Padding = (short)0;
            sdbMessage.Flags = SequoiadbConstants.DEFAULT_FLAGS;
            sdbMessage.ContextIDList = new List<long>();
            sdbMessage.ContextIDList.Add(_contextID);
            sdbMessage.BsonLen = 0;
            // MsgLobTuple
            sdbMessage.LobLen = (uint)b.Length;
            sdbMessage.LobSequence = SDB_LOB_DEFAULT_SEQ;
            sdbMessage.LobOffset = _readOffset;

            // build send msg
            byte[] request = SDBMessageHelper.BuildReadLobRequest(sdbMessage, _isBigEndian);
            // send msg
            _connection.SendMessage(request);

            // receive and extract return msg from engine
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReadLobReply(_connection.ReceiveMessage(_isBigEndian), _isBigEndian);

            // check the result
            int flags = rtnSDBMessage.Flags;
            int errCode = new BaseException("SDB_DMS_EOC").ErrorCode;
            if ( errCode == flags)
            {
                return -1;
            }
            else if (0 != flags)
            {
                throw new BaseException(flags);
            }
            // return the result
            byte[] lobData = rtnSDBMessage.LobBuff;
            Array.Copy(lobData, b, lobData.Length);

            _readOffset += lobData.Length;
            return lobData.Length;
        }

	}
}
