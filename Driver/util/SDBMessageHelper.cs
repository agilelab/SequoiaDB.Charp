using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using SequoiaDB.Bson;

namespace SequoiaDB.Driver
{
    class SDBMessageHelper
    {
        // msg.h
        private const int MESSAGE_HEADER_LENGTH = 28;
        private const int MESSAGE_OPQUERY_LENGTH = 61;
        private const int MESSAGE_OPINSERT_LENGTH = 45;
        private const int MESSAGE_OPDELETE_LENGTH = 45;
        private const int MESSAGE_OPUPDATE_LENGTH = 45;
        private const int MESSAGE_OPGETMORE_LENGTH = 40;
        private const int MESSAGE_KILLCURSOR_LENGTH = 36;
        private const int MESSAGE_OPLOB_LENGTH = 52;
        private const int MESSAGE_LOBTUPLE_LENGTH = 16;

        private static readonly Logger logger = new Logger("SDBMessageHelper");

        internal static SDBMessage CheckRetMsgHeader(SDBMessage sendMsg, SDBMessage rtnMsg)
        {
            uint sendOpCode = (uint)sendMsg.OperationCode;
            uint recvOpCode = (uint)rtnMsg.OperationCode;
            if ((sendOpCode | 0x80000000) != recvOpCode)
                rtnMsg.Flags = (int)Errors.errors.SDB_UNEXPECTED_RESULT;
            return rtnMsg;
        }

        internal static byte[] BuildQueryRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            string collectionName = sdbMessage.CollectionFullName;
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            ulong requestID = sdbMessage.RequestID;
            long skipRowsCount = sdbMessage.SkipRowsCount;
            long returnRowsCount = sdbMessage.ReturnRowsCount;
            byte[] collByteArray = System.Text.Encoding.UTF8.GetBytes(collectionName);
            int collectionNameLength = collByteArray.Length;

            byte[] query = sdbMessage.Matcher.ToBson();
            byte[] fieldSelector = sdbMessage.Selector.ToBson();
            byte[] orderBy = sdbMessage.OrderBy.ToBson();
            byte[] hint = sdbMessage.Hint.ToBson();
            byte[] nodeID = sdbMessage.NodeID;
            if (isBigEndian)
            {
                BsonEndianConvert(query, 0, query.Length, true);
                BsonEndianConvert(fieldSelector, 0, fieldSelector.Length, true);
                BsonEndianConvert(orderBy, 0, orderBy.Length, true);
                BsonEndianConvert(hint, 0, hint.Length, true);
            }

            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_OPQUERY_LENGTH + collectionNameLength, 4)
                + Helper.RoundToMultipleXLength(query.Length, 4)
                + Helper.RoundToMultipleXLength(fieldSelector.Length, 4)
                + Helper.RoundToMultipleXLength(orderBy.Length, 4)
                + Helper.RoundToMultipleXLength(hint.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(32);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushInt(collectionNameLength);
            buf.PushLong(skipRowsCount);
            buf.PushLong(returnRowsCount);

            fieldList.Add(buf.ToByteArray());

            byte[] newCollectionName = new byte[collectionNameLength + 1];
            for (int i = 0; i < collectionNameLength; i++)
                newCollectionName[i] = collByteArray[i];

            fieldList.Add(Helper.RoundToMultipleX(newCollectionName, 4));
            fieldList.Add(Helper.RoundToMultipleX(query, 4));
            fieldList.Add(Helper.RoundToMultipleX(fieldSelector, 4));
            fieldList.Add(Helper.RoundToMultipleX(orderBy, 4));
            fieldList.Add(Helper.RoundToMultipleX(hint, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled) {
               StringWriter buff = new StringWriter();
               foreach (byte by in msgInByteArray) {
                  buff.Write(string.Format("{0:X}", by));
               }
               logger.Debug("Query Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildInsertRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            string collectionName = sdbMessage.CollectionFullName;
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            ulong requestID = sdbMessage.RequestID;
            byte[] collByteArray = System.Text.Encoding.UTF8.GetBytes(collectionName);
            int collectionNameLength = collByteArray.Length;

            byte[] insertor = sdbMessage.Insertor.ToBson();
            byte[] nodeID = sdbMessage.NodeID;

            if (isBigEndian)
            {
                BsonEndianConvert(insertor, 0, insertor.Length, true);
            }
            // calculate the total length of the packet which to send 
            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_OPINSERT_LENGTH + collectionNameLength, 4)
                + Helper.RoundToMultipleXLength(insertor.Length, 4);
            // put all the part of packet into a list, and then transform the list into byte[]
            // we need byte[] while sending
            List<byte[]> fieldList = new List<byte[]>();
            // let's put the packet head into list
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(16);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushInt(collectionNameLength);

            fieldList.Add(buf.ToByteArray());
            // cl name also in the packet head, we need one more byte for '\0'
            byte[] newCollectionName = new byte[collectionNameLength + 1];
            for (int i = 0; i < collectionNameLength; i++)
                newCollectionName[i] = collByteArray[i];

            fieldList.Add(Helper.RoundToMultipleX(newCollectionName, 4));
            // we have finish preparing packet head
            // let's put the content into packet
            fieldList.Add(Helper.RoundToMultipleX(insertor, 4));
            // transform the list into byte[]
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Insert Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] AppendInsertMsg(byte[] msg, BsonDocument append, bool isBigEndian)
        {
            List<byte[]> tmp = Helper.SplitByteArray(msg, 4);
            byte[] msgLength = tmp[0];
            byte[] remainning = tmp[1];
            byte[] insertor = append.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(insertor, 0, insertor.Length, true);
            }
            int length = Helper.ByteToInt(msgLength, isBigEndian);
            int messageLength = length + Helper.RoundToMultipleXLength(insertor.Length, 4);

            ByteBuffer buf = new ByteBuffer(messageLength);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(messageLength);
            buf.PushByteArray(remainning);
            buf.PushByteArray(Helper.RoundToMultipleX(insertor, 4));

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in insertor)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("BulkInsert Append string==>" + buff.ToString() + "<==");
            }
            return buf.ToByteArray();
        }

        internal static byte[] BuildDeleteRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            string collectionName = sdbMessage.CollectionFullName;
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            ulong requestID = sdbMessage.RequestID;
            byte[] nodeID = sdbMessage.NodeID;
            byte[] collByteArray = System.Text.Encoding.UTF8.GetBytes(collectionName);
            int collectionNameLength = collByteArray.Length;

            byte[] matcher = sdbMessage.Matcher.ToBson();
            byte[] hint = sdbMessage.Hint.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(matcher, 0, matcher.Length, true);
                BsonEndianConvert(hint, 0, hint.Length, true);
            }

            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_OPDELETE_LENGTH + collectionNameLength, 4)
                + Helper.RoundToMultipleXLength(matcher.Length, 4)
                + Helper.RoundToMultipleXLength(hint.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(16);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushInt(collectionNameLength);

            fieldList.Add(buf.ToByteArray());

            byte[] newCollectionName = new byte[collectionNameLength + 1];
            for (int i = 0; i < collectionNameLength; i++)
                newCollectionName[i] = collByteArray[i];

            fieldList.Add(Helper.RoundToMultipleX(newCollectionName, 4));
            fieldList.Add(Helper.RoundToMultipleX(matcher, 4));
            fieldList.Add(Helper.RoundToMultipleX(hint, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Delete Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildUpdateRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            string collectionName = sdbMessage.CollectionFullName;
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            ulong requestID = sdbMessage.RequestID;
            byte[] nodeID = sdbMessage.NodeID;
            byte[] collByteArray = System.Text.Encoding.UTF8.GetBytes(collectionName);
            int collectionNameLength = collByteArray.Length;

            byte[] matcher = sdbMessage.Matcher.ToBson();
            byte[] hint = sdbMessage.Hint.ToBson();
            byte[] modifier = sdbMessage.Modifier.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(matcher, 0, matcher.Length, true);
                BsonEndianConvert(modifier, 0, modifier.Length, true);
                BsonEndianConvert(hint, 0, hint.Length, true);
            }

            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_OPUPDATE_LENGTH + collectionNameLength, 4)
                + Helper.RoundToMultipleXLength(matcher.Length, 4)
                + Helper.RoundToMultipleXLength(hint.Length, 4)
                + Helper.RoundToMultipleXLength(modifier.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(16);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushInt(collectionNameLength);

            fieldList.Add(buf.ToByteArray());

            byte[] newCollectionName = new byte[collectionNameLength + 1];
            for (int i = 0; i < collectionNameLength; i++)
                newCollectionName[i] = collByteArray[i];

            fieldList.Add(Helper.RoundToMultipleX(newCollectionName, 4));
            fieldList.Add(Helper.RoundToMultipleX(matcher, 4));
            fieldList.Add(Helper.RoundToMultipleX(modifier, 4));
            fieldList.Add(Helper.RoundToMultipleX(hint, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Update Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildSqlMsg(SDBMessage sdbMessage, string sql, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            ulong requestID = sdbMessage.RequestID;
            byte[] nodeID = sdbMessage.NodeID;
            byte[] sqlBytes = System.Text.Encoding.UTF8.GetBytes(sql);
            int sqlLen = sqlBytes.Length+1;
            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_HEADER_LENGTH + sqlLen, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            byte[] newArray = new byte[sqlLen];
            for (int i = 0; i < sqlLen - 1; i++)
                newArray[i] = sqlBytes[i];

            fieldList.Add(Helper.RoundToMultipleX(newArray, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("SQL Message string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildAuthMsg(SDBMessage sdbMessage, string username, string passwd, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            ulong requestID = sdbMessage.RequestID; 
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            BsonDocument auth = new BsonDocument();
            auth.Add(SequoiadbConstants.SDB_AUTH_USER, username);
            auth.Add(SequoiadbConstants.SDB_AUTH_PASSWD, passwd);
            byte[] authbyte = auth.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(authbyte, 0, authbyte.Length, true);
            }

            int messageLength = MESSAGE_HEADER_LENGTH + Helper.RoundToMultipleXLength(
                                authbyte.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID,
                                         opCode, isBigEndian));

            fieldList.Add(Helper.RoundToMultipleX(authbyte, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("SQL Message string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildAuthCrtMsg(SDBMessage sdbMessage , string username, string passwd, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode ;
            ulong requestID = sdbMessage.RequestID;
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            BsonDocument auth = new BsonDocument();
            auth.Add(SequoiadbConstants.SDB_AUTH_USER, username);
            auth.Add(SequoiadbConstants.SDB_AUTH_PASSWD, passwd);
            byte[] authbyte = auth.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(authbyte, 0, authbyte.Length, true);
            }

            int messageLength = MESSAGE_HEADER_LENGTH + Helper.RoundToMultipleXLength(
                                authbyte.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID,
                                         opCode, isBigEndian));

            fieldList.Add(Helper.RoundToMultipleX(authbyte, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("SQL Message string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildAuthDelMsg(SDBMessage sdbMessage, string username, string passwd, bool isBigEndian)
        {
            ulong requestID = sdbMessage.RequestID;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            BsonDocument auth = new BsonDocument();
            auth.Add(SequoiadbConstants.SDB_AUTH_USER, username);
            auth.Add(SequoiadbConstants.SDB_AUTH_PASSWD, passwd);
            byte[] authbyte = auth.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(authbyte, 0, authbyte.Length, true);
            }

            int messageLength = MESSAGE_HEADER_LENGTH + Helper.RoundToMultipleXLength(
                                authbyte.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID,
                                         opCode, isBigEndian));

            fieldList.Add(Helper.RoundToMultipleX(authbyte, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("SQL Message string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildDisconnectRequest(bool isBigEndian)
        {
            ulong requestID = 0;
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            int messageLength = Helper.RoundToMultipleXLength(MESSAGE_HEADER_LENGTH, 4);

            byte[] msgInByteArray = AssembleHeader(messageLength, requestID, nodeID, (int)Operation.OP_DISCONNECT, isBigEndian);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Disconnect Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildKillCursorMsg(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            List<long> contextIDs = sdbMessage.ContextIDList;
            ulong requestID = 0;
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            int lenContextIDs = sizeof(long) * contextIDs.Count;
            int messageLength = MESSAGE_KILLCURSOR_LENGTH + lenContextIDs;

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(8 + lenContextIDs);
            if (isBigEndian)
                buf.IsBigEndian = true;
            int zero = 0;
            int numContexts = 1;
            buf.PushInt(zero);
            buf.PushInt(numContexts);
            foreach (long contextID in contextIDs)
                buf.PushLong(contextID);

            fieldList.Add(buf.ToByteArray());
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Disconnect Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildTransactionRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opcode = (int)sdbMessage.OperationCode;
            ulong requestID = sdbMessage.RequestID;
            byte[] nodeID = SequoiadbConstants.ZERO_NODEID;
            int messageLength = Helper.RoundToMultipleXLength(MESSAGE_HEADER_LENGTH, 4);

            byte[] msgInByteArray = AssembleHeader(messageLength, requestID, nodeID, (int)opcode, isBigEndian);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Disconnect Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildGetMoreRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            ulong requestID = sdbMessage.RequestID;
            long contextId = sdbMessage.ContextIDList[0];
            int numReturned = sdbMessage.NumReturned;
            byte[] nodeID = sdbMessage.NodeID;

            int messageLength = MESSAGE_OPGETMORE_LENGTH;

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(12);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushLong(contextId);
            buf.PushInt(numReturned);
            fieldList.Add(buf.ToByteArray());

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("GetMore Request string==>" + buff.ToString() + "<==");
            }

            return msgInByteArray;
        }

	    internal static byte[] BuildAggrRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            int opCode = (int)sdbMessage.OperationCode;
            string collectionName = sdbMessage.CollectionFullName;
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            ulong requestID = sdbMessage.RequestID;
            byte[] collByteArray = System.Text.Encoding.UTF8.GetBytes(collectionName);
            int collectionNameLength = collByteArray.Length;

            byte[] insertor = sdbMessage.Insertor.ToBson();
            byte[] nodeID = sdbMessage.NodeID;

            if (isBigEndian)
            {
                BsonEndianConvert(insertor, 0, insertor.Length, true);
            }

            int messageLength = Helper.RoundToMultipleXLength(
                MESSAGE_OPINSERT_LENGTH + collectionNameLength, 4)
                + Helper.RoundToMultipleXLength(insertor.Length, 4);

            List<byte[]> fieldList = new List<byte[]>();
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            ByteBuffer buf = new ByteBuffer(16);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushInt(collectionNameLength);

            fieldList.Add(buf.ToByteArray());

            byte[] newCollectionName = new byte[collectionNameLength + 1];
            for (int i = 0; i < collectionNameLength; i++)
                newCollectionName[i] = collByteArray[i];

            fieldList.Add(Helper.RoundToMultipleX(newCollectionName, 4));
            fieldList.Add(Helper.RoundToMultipleX(insertor, 4));

            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Aggregate Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] AppendAggrMsg(byte[] msg, BsonDocument append, bool isBigEndian)
        {
            List<byte[]> tmp = Helper.SplitByteArray(msg, 4);
            byte[] msgLength = tmp[0];
            byte[] remainning = tmp[1];
            byte[] insertor = append.ToBson();
            if (isBigEndian)
            {
                BsonEndianConvert(insertor, 0, insertor.Length, true);
            }
            int length = Helper.ByteToInt(msgLength, isBigEndian);
            int messageLength = length + Helper.RoundToMultipleXLength(insertor.Length, 4);

            ByteBuffer buf = new ByteBuffer(messageLength);
            if (isBigEndian)
                buf.IsBigEndian = true;
            buf.PushInt(messageLength);
            buf.PushByteArray(remainning);
            buf.PushByteArray(Helper.RoundToMultipleX(insertor, 4));           

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in insertor)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Aggregate Append string==>" + buff.ToString() + "<==");
            }
            return buf.ToByteArray();
        }

        internal static byte[] BuildKillAllContextsRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            return BuildTransactionRequest(sdbMessage, isBigEndian);
        }

        internal static byte[] BuildOpenLobRequest(SDBMessage sdbMessage, bool isBigEndian)
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
            // get info to build _MsgOpLob
            // MsgHeader
            int messageLength = 0;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = sdbMessage.NodeID;
            ulong requestID = sdbMessage.RequestID;
            // the rest part of _MsgOpLOb
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            long contextID = sdbMessage.ContextIDList[0];
            uint bsonLen = 0;
            byte[] bLob = sdbMessage.Matcher.ToBson();
            bsonLen = (uint)bLob.Length;
            if (isBigEndian)
            {
                BsonEndianConvert(bLob, 0, bLob.Length, true);
            }
            // calculate total length
            messageLength = MESSAGE_OPLOB_LENGTH +
                            Helper.RoundToMultipleXLength(bLob.Length, 4);
            // build a array list for return
            List<byte[]> fieldList = new List<byte[]>();
            // add MsgHead
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            // add the rest part of MsgOpLob
            ByteBuffer buf = new ByteBuffer(MESSAGE_OPLOB_LENGTH - MESSAGE_HEADER_LENGTH);
            if (isBigEndian)
            {
                buf.IsBigEndian = true;
            }
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushLong(contextID);
            buf.PushInt((int)bsonLen);
            fieldList.Add(buf.ToByteArray());
            // add msg body
            fieldList.Add(Helper.RoundToMultipleX(bLob, 4));
            // convert to byte array and return
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Open Lob Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildCloseLobRequest(SDBMessage sdbMessage, bool isBigEndian)
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
            // get info to build _MsgOpLob
            // MsgHeader
            int messageLength = MESSAGE_OPLOB_LENGTH;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = sdbMessage.NodeID;
            ulong requestID = sdbMessage.RequestID;
            // the rest part of _MsgOpLOb
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            long contextID = sdbMessage.ContextIDList[0];
            uint bsonLen = sdbMessage.BsonLen;

            // build a array list for return
            List<byte[]> fieldList = new List<byte[]>();
            // add MsgHead
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            // add the rest part of MsgOpLob
            ByteBuffer buf = new ByteBuffer(MESSAGE_OPLOB_LENGTH - MESSAGE_HEADER_LENGTH);
            if (isBigEndian)
            {
                buf.IsBigEndian = true;
            }
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushLong(contextID);
            buf.PushInt((int)bsonLen);
            fieldList.Add(buf.ToByteArray());

            // convert to byte array and return
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Close Lob Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildReadLobRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            /*
                /// read req msg is |MsgOpLob|_MsgLobTuple|
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
            // get info to build _MsgOpLob
            // MsgHeader
            int messageLength = 0;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = sdbMessage.NodeID;
            ulong requestID = sdbMessage.RequestID;
            // the rest part of _MsgOpLOb
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            long contextID = sdbMessage.ContextIDList[0];
            uint bsonLen = sdbMessage.BsonLen;
            // MsgLobTuple
            uint length = sdbMessage.LobLen;
            uint sequence = sdbMessage.LobSequence;
            long offset = sdbMessage.LobOffset;
            // calculate total length
            messageLength = MESSAGE_OPLOB_LENGTH + MESSAGE_LOBTUPLE_LENGTH;
            // build a array list for return
            List<byte[]> fieldList = new List<byte[]>();
            // add MsgHead
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            // add the rest part of MsgOpLob and MsgLobTuple
            ByteBuffer buf = new ByteBuffer(MESSAGE_OPLOB_LENGTH - MESSAGE_HEADER_LENGTH + MESSAGE_LOBTUPLE_LENGTH);
            if (isBigEndian)
            {
                buf.IsBigEndian = true;
            }
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushLong(contextID);
            buf.PushInt((int)bsonLen);

            buf.PushInt((int)length);
            buf.PushInt((int)sequence);
            buf.PushLong(offset);
            fieldList.Add(buf.ToByteArray());
            // convert to byte array and return
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Read Lob Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildWriteLobRequest(SDBMessage sdbMessage, byte[] data, bool isBigEndian)
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
            // get info to build _MsgOpLob
            // MsgHeader
            int messageLength = 0;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = sdbMessage.NodeID;
            ulong requestID = sdbMessage.RequestID;
            // the rest part of _MsgOpLOb
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            long contextID = sdbMessage.ContextIDList[0];
            uint bsonLen = sdbMessage.BsonLen;
            // MsgLobTuple
            uint length = sdbMessage.LobLen;
            uint sequence = sdbMessage.LobSequence;
            long offset = sdbMessage.LobOffset;
            // calculate total length
            messageLength = MESSAGE_OPLOB_LENGTH
                            + MESSAGE_LOBTUPLE_LENGTH
                            + Helper.RoundToMultipleXLength(data.Length, 4);
            // build a array list for return
            List<byte[]> fieldList = new List<byte[]>();
            // add MsgHead
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            // add the rest part of MsgOpLob and MsgLobTuple
            ByteBuffer buf = new ByteBuffer(MESSAGE_OPLOB_LENGTH - MESSAGE_HEADER_LENGTH + MESSAGE_LOBTUPLE_LENGTH);
            if (isBigEndian)
            {
                buf.IsBigEndian = true;
            }
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushLong(contextID);
            buf.PushInt((int)bsonLen);

            buf.PushInt((int)length);
            buf.PushInt((int)sequence);
            buf.PushLong(offset);
            // add msg header
            fieldList.Add(buf.ToByteArray());
            // add msg body
            fieldList.Add(Helper.RoundToMultipleX(data, 4));
            // convert to byte array and return
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Write Lob Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static byte[] BuildRemoveLobRequest(SDBMessage sdbMessage, bool isBigEndian)
        {
            /*
                /// remove lob reg msg is |MsgOpLob|bsonobj|
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
            // get info to build _MsgOpLob
            // MsgHeader
            int messageLength = 0;
            int opCode = (int)sdbMessage.OperationCode;
            byte[] nodeID = sdbMessage.NodeID;
            ulong requestID = sdbMessage.RequestID;
            // the rest part of _MsgOpLOb
            int version = sdbMessage.Version;
            short w = sdbMessage.W;
            short padding = sdbMessage.Padding;
            int flags = sdbMessage.Flags;
            long contextID = sdbMessage.ContextIDList[0];
            uint bsonLen = 0;
            byte[] bLob = sdbMessage.Matcher.ToBson();
            bsonLen = (uint)bLob.Length;
            if (isBigEndian)
            {
                BsonEndianConvert(bLob, 0, bLob.Length, true);
            }
            // calculate total length
            messageLength = MESSAGE_OPLOB_LENGTH +
                            Helper.RoundToMultipleXLength(bLob.Length, 4);

            // build a array list for return
            List<byte[]> fieldList = new List<byte[]>();
            // add MsgHead
            fieldList.Add(AssembleHeader(messageLength, requestID, nodeID, opCode, isBigEndian));
            // add the rest part of MsgOpLob
            ByteBuffer buf = new ByteBuffer(MESSAGE_OPLOB_LENGTH - MESSAGE_HEADER_LENGTH);
            if (isBigEndian)
            {
                buf.IsBigEndian = true;
            }
            buf.PushInt(version);
            buf.PushShort(w);
            buf.PushShort(padding);
            buf.PushInt(flags);
            buf.PushLong(contextID);
            buf.PushInt((int)bsonLen);
            // add msg header
            fieldList.Add(buf.ToByteArray());
            // add msg body
            fieldList.Add(Helper.RoundToMultipleX(bLob, 4));

            // convert to byte array and return
            byte[] msgInByteArray = Helper.ConcatByteArray(fieldList);

            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in msgInByteArray)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Remove Lob Request string==>" + buff.ToString() + "<==");
            }
            return msgInByteArray;
        }

        internal static SDBMessage MsgExtractReply(byte[] inBytes, bool isBigEndian)
        {
            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in inBytes)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Hex String got from server, to be extracted==>" + buff.ToString() + "<==");
            }

            List<byte[]> tmp = Helper.SplitByteArray(inBytes, MESSAGE_HEADER_LENGTH);
            byte[] header = tmp[0];
            byte[] remaining = tmp[1];

            if (header.Length != MESSAGE_HEADER_LENGTH || remaining == null)
                throw new BaseException("SDB_INVALIDSIZE");

            SDBMessage sdbMessage = new SDBMessage();
            ExtractHeader(sdbMessage, header, isBigEndian);

            tmp = Helper.SplitByteArray(remaining, 8);
            byte[] contextID = tmp[0];
            remaining = tmp[1];

            List<long> contextIDList = new List<long>();
            contextIDList.Add(Helper.ByteToLong(contextID, isBigEndian));
            sdbMessage.ContextIDList = contextIDList;

            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] flags = tmp[0];
            remaining = tmp[1];
            sdbMessage.Flags = Helper.ByteToInt(flags, isBigEndian);

            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] startFrom = tmp[0];
            remaining = tmp[1];
            sdbMessage.StartFrom = Helper.ByteToInt(startFrom, isBigEndian);

            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] returnRows = tmp[0];
            remaining = tmp[1];
            int numReturned = Helper.ByteToInt(returnRows, isBigEndian);
            sdbMessage.NumReturned = numReturned;

            if (numReturned > 0)
            {
                List<BsonDocument> objList = ExtractBsonObject(remaining, isBigEndian);
                sdbMessage.ObjectList = objList;
            }

            return sdbMessage;
        }

        internal static SDBMessage MsgExtractReadLobReply(byte[] inBytes, bool isBigEndian)
        {
            /*
                // read res msg is |MsgOpReply|_MsgLobTuple|data|
                struct _MsgOpReply
                {
                   // 0-27 bytes
                   MsgHeader header ;     // message header
                   // 28-31 bytes
                   SINT64    contextID ;   // context id if client need to get more
                   // 32-35 bytes
                   SINT32    flags ;      // reply flags
                   // 36-39 bytes
                   SINT32    startFrom ;  // where in the context "this" reply is starting
                   // 40-43 bytes
                   SINT32    numReturned ;// number of recourds returned in the reply
                } ;
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
            if (logger.IsDebugEnabled)
            {
                StringWriter buff = new StringWriter();
                foreach (byte by in inBytes)
                {
                    buff.Write(string.Format("{0:X}", by));
                }
                logger.Debug("Hex String for read lob got from server, to be extracted==>" + buff.ToString() + "<==");
            }

            List<byte[]> tmp = Helper.SplitByteArray(inBytes, MESSAGE_HEADER_LENGTH);
            byte[] header = tmp[0];
            byte[] remaining = tmp[1];

            if (header.Length != MESSAGE_HEADER_LENGTH || remaining == null)
                throw new BaseException("SDB_INVALIDSIZE");

            SDBMessage sdbMessage = new SDBMessage();
            /// extract info from _MsgOpReply
            // MsgHeader
            ExtractHeader(sdbMessage, header, isBigEndian);
            // contextID
            tmp = Helper.SplitByteArray(remaining, 8);
            byte[] contextID = tmp[0];
            remaining = tmp[1];

            List<long> contextIDList = new List<long>();
            contextIDList.Add(Helper.ByteToLong(contextID, isBigEndian));
            sdbMessage.ContextIDList = contextIDList;
            // flags
            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] flags = tmp[0];
            remaining = tmp[1];
            sdbMessage.Flags = Helper.ByteToInt(flags, isBigEndian);
            // startFrom
            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] startFrom = tmp[0];
            remaining = tmp[1];
            sdbMessage.StartFrom = Helper.ByteToInt(startFrom, isBigEndian);
            // numReturned
            tmp = Helper.SplitByteArray(remaining, 4);
            byte[] returnRows = tmp[0];
            remaining = tmp[1];
            int numReturned = Helper.ByteToInt(returnRows, isBigEndian);
            sdbMessage.NumReturned = numReturned;
            sdbMessage.ObjectList = null;
            /// extract info from _MsgLobTuple
            // if nothing wrong, we are going to extract MsgLobTuple
            if (0 == sdbMessage.Flags)
            { 
                // lob len
                tmp = Helper.SplitByteArray(remaining, 4);
                byte[] lobLen = tmp[0];
                remaining = tmp[1];
                sdbMessage.LobLen = (uint)Helper.ByteToInt(lobLen, isBigEndian);
                // lob sequence
                tmp = Helper.SplitByteArray(remaining, 4);
                byte[] lobSequence = tmp[0];
                remaining = tmp[1];
                sdbMessage.LobSequence = (uint)Helper.ByteToInt(lobSequence, isBigEndian);
                // lob offset
                tmp = Helper.SplitByteArray(remaining, 8);
                byte[] lobOffset = tmp[0];
                remaining = tmp[1];
                sdbMessage.LobOffset = (uint)Helper.ByteToLong(lobOffset, isBigEndian);
                // set lob buff
                byte[] buff = new byte[sdbMessage.LobLen];
                Array.Copy(remaining, buff, remaining.Length);
                sdbMessage.LobBuff = buff;
            }
            
            return sdbMessage;
        }

        internal static byte[] BuildSysInfoRequest()
        {
            ByteBuffer buf = new ByteBuffer(12);
            buf.PushByteArray(BitConverter.GetBytes(SequoiadbConstants.MSG_SYSTEM_INFO_LEN));
            buf.PushByteArray(BitConverter.GetBytes(SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER));
            buf.PushInt(12);

            return buf.ToByteArray();
        }

        internal static bool ExtractSysInfoReply(byte[] inBytes, ref int osType )
        {
            bool endian;
            UInt32 eyeCatcher = BitConverter.ToUInt32( inBytes, 4 );

            if (eyeCatcher == SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER)
                endian = false;
            else if (eyeCatcher == SequoiadbConstants.MSG_SYSTEM_INFO_EYECATCHER_REVERT)
                endian = true;
            else
                throw new BaseException("SDB_INVALIDARG");

            if (osType != 0)
            {
                if (endian)
                {
                    byte[] tmp = new byte[4];
                    Array.Copy(inBytes, 12, tmp, 0, 4);
                    Array.Reverse(tmp);
                    osType = BitConverter.ToInt32(tmp, 0);
                }
                else
                    osType = BitConverter.ToInt32(inBytes, 12);
            }

            return endian;
        }

        private static byte[] AssembleHeader(int messageLength, ulong requestID,
                                             byte[] nodeID, int operationCode, bool isBigEndian)
        {
            ByteBuffer buf = new ByteBuffer(MESSAGE_HEADER_LENGTH);
            if (isBigEndian)
                buf.IsBigEndian = true;

            buf.PushInt(messageLength);
            buf.PushInt(operationCode);
            buf.PushByteArray(nodeID);
            buf.PushLong((long)requestID);

            return buf.ToByteArray();
        }

        private static void ExtractHeader(SDBMessage sdbMessage, byte[] header, bool isBigEndian)
        {
            List<byte[]> tmp = Helper.SplitByteArray(header, 4);
            byte[] msgLength = tmp[0];
            byte[] remainning = tmp[1];
            sdbMessage.RequestLength = Helper.ByteToInt(msgLength, isBigEndian);

            tmp = Helper.SplitByteArray(remainning, 4);
            byte[] opCode = tmp[0];
            remainning = tmp[1];
            sdbMessage.OperationCode = (Operation)Helper.ByteToInt(opCode, isBigEndian);

            tmp = Helper.SplitByteArray(remainning, 12);
            byte[] nodeID = tmp[0];
            remainning = tmp[1];
            sdbMessage.NodeID = nodeID;
            sdbMessage.RequestID = (ulong)Helper.ByteToLong(remainning, isBigEndian); 
        }

        private static List<BsonDocument> ExtractBsonObject(byte[] inBytes, bool isBigEndian)
        {
            int objLen;
            int objAllotLen;
            byte[] remaining = inBytes;
            List<BsonDocument> objList = new List<BsonDocument>();
            while (remaining != null)
            {
               objLen = GetBsonObjectLength(remaining, 0, isBigEndian);
               if (objLen <= 0 || objLen > remaining.Length)
               {
                  logger.Error("Invalid length of BSONObject:::" + objLen);
                  if (logger.IsDebugEnabled) {
                        StringWriter buff = new StringWriter();
                        foreach (byte by in inBytes)
                        {
                            buff.Write(string.Format("{0:X}", by));
                        }
                       logger.Debug("BsonObject Hex String==>" + buff.ToString() + "<==");
                  }
                  throw new BaseException("SDB_INVALIDSIZE"); 
               }
                objAllotLen = Helper.RoundToMultipleXLength(objLen, 4);

                List<byte[]> tmp = Helper.SplitByteArray(remaining, objAllotLen);
                byte[] obj = tmp[0];
                if ( isBigEndian )
                    BsonEndianConvert(obj, 0, objLen, false) ;
                remaining = tmp[1];

                BsonDocument bson = BsonDocument.ReadFrom(obj);
                objList.Add(bson);
            }

            return objList;
        }

        private static int GetBsonObjectLength(byte[] inBytes, int offset, bool isBigEndian)
        {
            byte[] tmp = new byte[4];
            for (int i = 0; i < 4; i++)
                tmp[i] = inBytes[offset+i];

            return Helper.ByteToInt(tmp, isBigEndian);
        }

        private static void BsonEndianConvert(byte[] inBytes, int offset, int objSize, bool l2r)
        {
            int beginOff = offset;
            Array.Reverse(inBytes, offset, 4);
            offset += 4;
            while (offset < inBytes.Length)
            {
                // get bson element type
                BsonType type = (BsonType)inBytes[offset];
                // move offset to next in order to skip type
                offset += 1;
                if (BsonType.EndOfDocument == type)
                    break;
                // skip element name, note that element name is a string ended up with '\0'
                offset += Strlen(inBytes, offset) + 1;
                switch (type)
                {
                    case BsonType.Double:
                        Array.Reverse(inBytes, offset, 8);
                        offset += 8;
                        break;

                    case BsonType.String:
                    case BsonType.JavaScript:
                    case BsonType.Symbol:
                    {
                     // for those 3 types, there are 4 bytes length plus a string
                     // the length is the length of string plus '\0'
                        int len = BitConverter.ToInt32(inBytes, offset);
                        Array.Reverse(inBytes, offset, 4);
                        int newlen = BitConverter.ToInt32(inBytes, offset);
                        offset += (l2r ? len : newlen) + 4;
                        break;
                    }

                    case BsonType.Document:
                    case BsonType.Array:
                    {
                        int objLen = GetBsonObjectLength(inBytes, offset, !l2r);
                        BsonEndianConvert(inBytes, offset, objLen, l2r);
                        offset += objLen;
                        break;
                    }

                    case BsonType.Binary:
                    {
                     // for bindata, there are 4 bytes length, 1 byte subtype and data
                     // note length is the real length of data
                        int len = BitConverter.ToInt32(inBytes, offset);
                        Array.Reverse(inBytes, offset, 4);
                        int newlen = BitConverter.ToInt32(inBytes, offset);
                        offset += (l2r ? len : newlen) + 5;
                        break;
                    }

                    case BsonType.Undefined:
                    case BsonType.Null:
                    case BsonType.MaxKey:
                    case BsonType.MinKey:
                     // nothing in those types
                        break;

                    case BsonType.ObjectId:
                        offset += 12;
                        break;

                    case BsonType.Boolean:
                        offset += 1;
                        break;

                    case BsonType.DateTime:
                        Array.Reverse(inBytes, offset, 8);
                        offset += 8;
                        break;

                    case BsonType.RegularExpression:
                     // two cstring, each with string
                        // for regex
                        offset += Strlen(inBytes, offset) + 1;
                        // for options
                        offset += Strlen(inBytes, offset) + 1;
                        break;
                    case BsonType.DBPointer:  //james.wei ToDo
                        {
                            // dbpointer is 4 bytes length + string + 12 bytes
                            int len = BitConverter.ToInt32(inBytes, offset);
                            Array.Reverse(inBytes, offset, 4);
                            int newlen = BitConverter.ToInt32(inBytes, offset);
                            offset += (l2r ? len : newlen) + 4;
                            offset += 12;
                            break;
                        }
                    case BsonType.JavaScriptWithScope:
                    {
                     // 4 bytes and 4 bytes + string, then obj
                        Array.Reverse(inBytes, offset, 4);
                        offset += 4;
                        // then string
                        int len = BitConverter.ToInt32(inBytes, offset);
                        Array.Reverse(inBytes, offset, 4);
                        int newlen = BitConverter.ToInt32(inBytes, offset);
                        offset += (l2r ? len : newlen) + 4;
                        // then object
                        int objLen = GetBsonObjectLength(inBytes, offset, !l2r);
                        BsonEndianConvert(inBytes, offset, objLen, l2r);
                        offset += objLen;
                        break;
                    }

                    case BsonType.Int32:
                        Array.Reverse(inBytes, offset, 4);
                        offset += 4;
                        break;

                    case BsonType.Int64:
                        Array.Reverse(inBytes, offset, 8);
                        offset += 8;
                        break;

                    case BsonType.Timestamp:
                     // timestamp is with 2 4-bytes
                        Array.Reverse(inBytes, offset, 4);
                        offset += 4;
                        Array.Reverse(inBytes, offset, 4);
                        offset += 4;
                        break;
                }
            }
            if (offset - beginOff != objSize )
                throw new BaseException("SDB_INVALIDSIZE");

        }

        private static int Strlen(byte[] str, int offset)
        {
            int len = 0;
            for (int i = offset; i < str.Length; i++)
                if (str[i] == '\0')
                    break;
                else
                    len++;
            return len;
        }
    }
}
