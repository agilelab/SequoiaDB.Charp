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
using System.Linq;

namespace SequoiaDB.Driver.Linq
{
    /// <summary>
    /// Static class that contains the Mongo Linq extension methods.
    /// </summary>
    public static class LinqExtensionMethods
    {
        /// <summary>
        /// Returns an instance of IQueryable{{T}} for a MongoCollection.
        /// </summary>
        /// <typeparam name="T">The type of the returned documents.</typeparam>
        /// <param name="collection">The name of the collection.</param>
        /// <returns>An instance of IQueryable{{T}} for a MongoCollection.</returns>
        public static IQueryable<T> AsQueryable<T>(this DBCollection collection)
        {
            var provider = new SequoiaQueryProvider(collection);
            return new SequoiaQueryable<T>(provider);
        }

        /// <summary>
        /// Returns an instance of IQueryable{{T}} for a MongoCollection.
        /// </summary>
        /// <typeparam name="T">The type of the returned documents.</typeparam>
        /// <param name="collection">The name of the collection.</param>
        /// <returns>An instance of IQueryable{{T}} for a MongoCollection.</returns>
        public static IQueryable<T> AsQueryable<T>(this DBCollection<T> collection)
        {
            var provider = new SequoiaQueryProvider(collection);
            return new SequoiaQueryable<T>(provider);
        }
    }
}
