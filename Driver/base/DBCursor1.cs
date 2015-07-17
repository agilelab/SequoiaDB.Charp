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
using SequoiaDB.Driver;

namespace SequoiaDB.Driver
{
    public class DBCursor<TDocument> : DBCursor, IEnumerable<TDocument>
    {
        DBCursorEnumerator<TDocument> m_Enumerator = null;

        internal DBCursor(SDBMessage rtnSDBMessage, DBCollection dbc)
            :base(rtnSDBMessage,dbc)
        {
            this.m_Enumerator = new DBCursorEnumerator<TDocument>(this);
        }

        protected override System.Collections.IEnumerator IEnumerableGetEnumerator()
        {
            return this.GetEnumerator();
        }

        #region IEnumerable<TDocument> 成员

        public IEnumerator<TDocument> GetEnumerator()
        {
            return this.m_Enumerator;
        }

        #endregion
    }
}
