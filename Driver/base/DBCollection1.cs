/* Copyright 2045 james.wei.
* http://www.cnblogs.com/eastjade/
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SequoiaDB.Bson;

namespace SequoiaDB.Driver
{
    public class DBCollection<TDocument> : DBCollection
    {
        internal DBCollection(CollectionSpace cs, string name)
            : base(cs, name)
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public Bson.BsonValue Insert(TDocument document)
        {
            BsonDocument insertor = new BsonDocument();
            using (SequoiaDB.Bson.IO.BsonWriter w = SequoiaDB.Bson.IO.BsonWriter.Create(insertor))
            {
                SequoiaDB.Bson.Serialization.BsonSerializer.Serialize<TDocument>(w, document);
            }

            return base.Insert(insertor);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="documents"></param>
        /// <param name="flag">FLG_INSERT_CONTONDUP or 0</param>
        public void BulkInsert(IEnumerable<TDocument> documents, int flag)
        {
            List<BsonDocument> vList = new List<BsonDocument>();
            foreach (var item in documents)
            {
                BsonDocument insertor = new BsonDocument();
                using (SequoiaDB.Bson.IO.BsonWriter w = SequoiaDB.Bson.IO.BsonWriter.Create(insertor))
                {
                    SequoiaDB.Bson.Serialization.BsonSerializer.Serialize<TDocument>(w, item);
                }
                vList.Add(insertor);
            }

            base.BulkInsert(vList, flag);
        }

        public override DBCursor Query(BsonDocument query, BsonDocument selector, BsonDocument orderBy, BsonDocument hint, long skipRows, long returnRows, int flag)
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
            SDBMessage rtnSDBMessage = AdminCommand(FullName, query, selector,
                                                    orderBy, hint, skipRows, returnRows, flag);

            int flags = rtnSDBMessage.Flags;
            if (flags != 0)
                if (flags == SequoiadbConstants.SDB_DMS_EOC)
                    return null;
                else
                {
                    throw new BaseException(flags);
                }

            return new DBCursor<TDocument>(rtnSDBMessage, this);
        }

        #region AdminCommand

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
            base.Connection.SendMessage(request);
            SDBMessage rtnSDBMessage = SDBMessageHelper.MsgExtractReply(base.Connection.ReceiveMessage(isBigEndian), isBigEndian);
            rtnSDBMessage = SDBMessageHelper.CheckRetMsgHeader(sdbMessage, rtnSDBMessage);
            return rtnSDBMessage;
        }

        #endregion

    }
}
