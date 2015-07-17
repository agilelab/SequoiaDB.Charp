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
using System.Linq;
using System.Linq.Expressions;
using SequoiaDB.Driver.Builders;
using System.Collections.Generic;
using SequoiaDB.Bson;
using SequoiaDB.Bson.IO;

namespace SequoiaDB.Driver.Linq
{
    /// <summary>
    /// james.wei 2015-07-25
    /// </summary>
    public static class LinqExtensions
    {
        /// <summary>
        /// Removes the specified collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="selector">The selector.</param>
        public static void Delete<T>(this DBCollection<T> collection, Expression<Func<T, bool>> selector) where T : class
        {
            collection.Delete((BsonDocument)GetQuery(collection, selector));
        }

        /// <summary>
        /// Updates the selectorified collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="updater">The updater.</param>
        /// <param name="selector">The selector.</param>
        public static void Update<T>(this DBCollection<T> collection, Expression<Func<T, T>> updater, Expression<Func<T, bool>> selector) where T : class
        {
            Dictionary<string, object> KV = new Dictionary<string, object>();

            #region //获取Update的赋值语句。

            var updateMemberExpr = (MemberInitExpression)updater.Body;
            var updateMembers = updateMemberExpr.Bindings.Cast<MemberAssignment>();
            foreach (var item in updateMembers)
            {
                string name = item.Member.Name;
                var value = Expression.Lambda(item.Expression).Compile().DynamicInvoke();
                if (!string.IsNullOrEmpty(name))
                    KV.Add(name, value);
            }

            #endregion

            var wrapper = new UpdateDocument(KV);
            BsonDocument modifier = new BsonDocument();
            modifier.Add("$set", (BsonDocument)wrapper);
            var mongoQuery = GetQuery(collection, selector);
            collection.Update((BsonDocument)mongoQuery, modifier, new BsonDocument());
        }

        /// <summary>
        /// Updates the selectorified collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="document">The document.</param>
        /// <param name="selector">The selector.</param>
        public static void Update<T>(this DBCollection<T> collection, T document, Expression<Func<T, bool>> selector) where T : class
        {
            BsonDocument updater = new BsonDocument();
            using (BsonWriter bsonWriter= BsonWriter.Create(updater))
            {
                SequoiaDB.Bson.Serialization.BsonSerializer.Serialize<T>(bsonWriter, document);
            }
            var mongoQuery = GetQuery(collection, selector);
            BsonDocument modifier = new BsonDocument();
            modifier.Add("$set", updater);
            collection.Update((BsonDocument)mongoQuery, modifier, new BsonDocument());
        }

        /// <summary>
        /// Gets the query.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="selector">The selector.</param>
        /// <returns></returns>
        private static ISequoiaQuery GetQuery<T>(DBCollection<T> collection, Expression<Func<T, bool>> selector) where T : class
        {
            var query= collection.AsQueryable<T>().Where(selector);
            var mongoQuery = ((SequoiaQueryable<T>)query).GetMongoQuery();
            return mongoQuery;
        }
    }
}