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
using System.Collections;
using System.Collections.Generic;
using SequoiaDB.Bson;

namespace SequoiaDB.Driver
{
    /// <summary>
    /// Reprsents an enumerator that fetches the results of a query sent to the server.
    /// </summary>
    /// <typeparam name="TDocument">The type of the documents returned.</typeparam>
    public class DBCursorEnumerator<TDocument> : IEnumerator<TDocument>
    {
        // private fields
        private readonly DBCursor<TDocument> _cursor;

        private bool _disposed = false;
        private TDocument m_Current;
        
        // constructors
        /// <summary>
        /// Initializes a new instance of the MongoCursorEnumerator class.
        /// </summary>
        /// <param name="cursor">The cursor to be enumerated.</param>
        public DBCursorEnumerator(DBCursor<TDocument> cursor)
        {
            _cursor = cursor;
        }

        // public properties
        /// <summary>
        /// Gets the current document.
        /// </summary>
        public TDocument Current
        {
            get
            {
                if (_disposed) { throw new ObjectDisposedException("DBCursorEnumerator"); }
                return m_Current;
            }
        }

        // public methods
        /// <summary>
        /// Disposes of any resources held by this enumerator.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                try
                {
                    try
                    {
                        this._cursor.KillCursor();
                    }
                    finally
                    {

                    }
                }
                finally
                {
                    _disposed = true;
                }
            }
        }

        /// <summary>
        /// Moves to the next result and returns true if another result is available.
        /// </summary>
        /// <returns>True if another result is available.</returns>
        public bool MoveNext()
        {
            if (_disposed) { throw new ObjectDisposedException("MongoCursorEnumerator"); }
            BsonDocument document = this._cursor.Next();
            if (document != null)
            {
                this.m_Current = SequoiaDB.Bson.Serialization.BsonSerializer.Deserialize<TDocument>(document);
            }
            else
            {
                this.m_Current = default(TDocument);
            }

            return document != null;
        }

        /// <summary>
        /// Resets the enumerator (not supported by MongoCursorEnumerator).
        /// </summary>
        public void Reset()
        {
            throw new NotSupportedException();
        }

        // explicit interface implementations
        object IEnumerator.Current
        {
            get { return Current; }
        }        
    }
}
