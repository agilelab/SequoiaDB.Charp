/* Copyright 2010-2014 MongoDB Inc.
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
using System.Linq.Expressions;
using SequoiaDB.Bson;
using SequoiaDB.Driver.Linq;
using SequoiaDB.Driver.Linq.Utils;

namespace SequoiaDB.Driver.Builders
{
    /// <summary>
    /// A builder for creating queries.
    /// </summary>
    public static class Query
    {
        // public static properties
        /// <summary>
        /// Gets a null value with a type of IMongoQuery.
        /// </summary>
        public static ISequoiaQuery Null
        {
            get { return null; }
        }

        // public static methods
        /// <summary>
        /// Tests that the named array element contains all of the values (see $all).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery All(string name, IEnumerable<BsonValue> values)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var condition = new BsonDocument("$all", new BsonArray(values));
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that all the queries are true (see $and in newer versions of the server).
        /// </summary>
        /// <param name="queries">A list of subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery And(IEnumerable<ISequoiaQuery> queries)
        {
            if (queries == null)
            {
                throw new ArgumentNullException("queries");
            }
            if (!queries.Any())
            {
                throw new ArgumentOutOfRangeException("queries", "And cannot be called with zero queries.");
            }

            var queryDocument = new QueryDocument();
            foreach (var query in queries)
            {
                if (query == null)
                {
                    throw new ArgumentOutOfRangeException("queries", "One of the queries is null.");
                }
                foreach (var clause in query.ToBsonDocument())
                {
                    AddAndClause(queryDocument, clause);
                }
            }

            return queryDocument;
        }

        /// <summary>
        /// Tests that all the queries are true (see $and in newer versions of the server).
        /// </summary>
        /// <param name="queries">A list of subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery And(params ISequoiaQuery[] queries)
        {
            return And((IEnumerable<ISequoiaQuery>)queries);
        }

        /// <summary>
        /// Tests that at least one item of the named array element matches a query (see $elemMatch).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="query">The query to match elements with.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery ElemMatch(string name, ISequoiaQuery query)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            var condition = new BsonDocument("$elemMatch", query.ToBsonDocument());
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that the value of the named element is equal to some value.
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery EQ(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, value);
        }

        /// <summary>
        /// Tests that an element of that name exists (see $exists).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Exists(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            return new QueryDocument(name, new BsonDocument("$exists", true));
        }

        /// <summary>
        /// Tests that the value of the named element is greater than some value (see $gt).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GT(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, new BsonDocument("$gt", value));
        }

        /// <summary>
        /// Tests that the value of the named element is greater than or equal to some value (see $gte).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GTE(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, new BsonDocument("$gte", value));
        }

        /// <summary>
        /// Tests that the value of the named element is equal to one of a list of values (see $in).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery In(string name, IEnumerable<BsonValue> values)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            return new QueryDocument(name, new BsonDocument("$in", new BsonArray(values)));
        }

        /// <summary>
        /// Tests that the value of the named element is less than some value (see $lt).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LT(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, new BsonDocument("$lt", value));
        }

        /// <summary>
        /// Tests that the value of the named element is less than or equal to some value (see $lte).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LTE(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, new BsonDocument("$lte", value));
        }

        /// <summary>
        /// Tests that the value of the named element matches a regular expression (see $regex).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="regex">The regex.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Matches(string name, BsonRegularExpression regex)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (regex == null)
            {
                throw new ArgumentNullException("regex");
            }

            return new QueryDocument(name, regex);
        }

        /// <summary>
        /// Tests that the modulus of the value of the named element matches some value (see $mod).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="modulus">The modulus.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Mod(string name, long modulus, long value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            BsonDocument condition;
            if (modulus >= int.MinValue && modulus <= int.MaxValue &&
                value >= int.MinValue && value <= int.MaxValue)
            {
                condition = new BsonDocument("$mod", new BsonArray { (int)modulus, (int)value });
            }
            else
            {
                condition = new BsonDocument("$mod", new BsonArray { modulus, value });
            }
            return new QueryDocument(name, condition);
        }
        
        /// <summary>
        /// Tests that the inverse of the query is true (see $not).
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Not(ISequoiaQuery query)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            return NegateQuery(query.ToBsonDocument());
        }

        /// <summary>
        /// Tests that an element does not equal the value (see $ne).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NE(string name, BsonValue value)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            return new QueryDocument(name, new BsonDocument("$ne", value));
        }

        /// <summary>
        /// Tests that an element of that name does not exist (see $exists).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NotExists(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            return new QueryDocument(name, new BsonDocument("$exists", false));
        }

        /// <summary>
        /// Tests that the value of the named element is not equal to any item in a list of values (see $nin).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="values">The values to compare.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NotIn(string name, IEnumerable<BsonValue> values)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            return new QueryDocument(name, new BsonDocument("$nin", new BsonArray(values)));
        }

        /// <summary>
        /// Tests that at least one of the subqueries is true (see $or).
        /// </summary>
        /// <param name="queries">The subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Or(IEnumerable<ISequoiaQuery> queries)
        {
            if (queries == null)
            {
                throw new ArgumentNullException("queries");
            }
            if (!queries.Any())
            {
                throw new ArgumentOutOfRangeException("queries", "Or cannot be called with zero queries.");
            }

            var queryArray = new BsonArray();
            foreach (var query in queries)
            {
                if (query == null)
                {
                    throw new ArgumentOutOfRangeException("queries", "One of the queries is null.");
                }

                // flatten out nested $or
                var queryDocument = query.ToBsonDocument();
                if (queryDocument.ElementCount == 1 && queryDocument.GetElement(0).Name == "$or")
                {
                    foreach (var nestedQuery in queryDocument[0].AsBsonArray)
                    {
                        queryArray.Add(nestedQuery);
                    }
                }
                else
                {
                    if (queryDocument.ElementCount != 0)
                    {
                        queryArray.Add(queryDocument);
                    }
                    else
                    {
                        // if any query is { } (which matches everything) then the overall Or matches everything also
                        return new QueryDocument();
                    }
                }
            }

            switch (queryArray.Count)
            {
                case 0:
                    return new QueryDocument(); // all queries were empty so just return an empty query
                case 1:
                    return new QueryDocument(queryArray[0].AsBsonDocument);
                default:
                    return new QueryDocument("$or", queryArray);
            }
        }

        /// <summary>
        /// Tests that at least one of the subqueries is true (see $or).
        /// </summary>
        /// <param name="queries">The subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Or(params ISequoiaQuery[] queries)
        {
            return Or((IEnumerable<ISequoiaQuery>)queries);
        }

        /// <summary>
        /// Tests that the size of the named array is equal to some value (see $size).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Size(string name, int size)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var condition = new BsonDocument("$size", size);
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that the size of the named array is greater than some value.
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery SizeGreaterThan(string name, int size)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var elementName = string.Format("{0}.{1}", name, size);
            var condition = new BsonDocument("$exists", true);
            return new QueryDocument(elementName, condition);
        }

        /// <summary>
        /// Tests that the size of the named array is greater than or equal to some value.
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery SizeGreaterThanOrEqual(string name, int size)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var elementName = string.Format("{0}.{1}", name, size - 1);
            var condition = new BsonDocument("$exists", true);
            return new QueryDocument(elementName, condition);
        }

        /// <summary>
        /// Tests that the size of the named array is less than some value.
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery SizeLessThan(string name, int size)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var elementName = string.Format("{0}.{1}", name, size - 1);
            var condition = new BsonDocument("$exists", false);
            return new QueryDocument(elementName, condition);
        }

        /// <summary>
        /// Tests that the size of the named array is less than or equal to some value.
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery SizeLessThanOrEqual(string name, int size)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var elementName = string.Format("{0}.{1}", name, size);
            var condition = new BsonDocument("$exists", false);
            return new QueryDocument(elementName, condition);
        }

        /// <summary>
        /// Tests that the type of the named element is equal to some type (see $type).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="type">The type to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Type(string name, BsonType type)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var condition = new BsonDocument("$type", (int)type);
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that a JavaScript expression is true (see $where).
        /// </summary>
        /// <param name="javascript">The javascript.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Where(BsonJavaScript javascript)
        {
            if (javascript == null)
            {
                throw new ArgumentNullException("javascript");
            }

            return new QueryDocument("$where", javascript);
        }

        /// <summary>
        /// Tests that the value of the named element is within a circle (see $within and $center).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="centerX">The x coordinate of the origin.</param>
        /// <param name="centerY">The y coordinate of the origin.</param>
        /// <param name="radius">The radius of the circle.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery WithinCircle(string name, double centerX, double centerY, double radius)
        {
            return WithinCircle(name, centerX, centerY, radius, false);
        }

        /// <summary>
        /// Tests that the value of the named element is within a circle (see $within and $center).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="centerX">The x coordinate of the origin.</param>
        /// <param name="centerY">The y coordinate of the origin.</param>
        /// <param name="radius">The radius of the circle.</param>
        /// <param name="spherical">if set to <c>true</c> [spherical].</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery WithinCircle(string name, double centerX, double centerY, double radius, bool spherical)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var shape = spherical ? "$centerSphere" : "$center";
            var condition = new BsonDocument("$within", new BsonDocument(shape, new BsonArray { new BsonArray { centerX, centerY }, radius }));
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that the value of the named element is within a polygon (see $within and $polygon).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="points">An array of points that defines the polygon (the second dimension must be of length 2).</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery WithinPolygon(string name, double[,] points)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            if (points == null)
            {
                throw new ArgumentNullException("points");
            }
            if (points.GetLength(1) != 2)
            {
                var message = string.Format("The second dimension of the points array must be of length 2, not {0}.", points.GetLength(1));
                throw new ArgumentOutOfRangeException("points", message);
            }

            var arrayOfPoints = new BsonArray(points.GetLength(0));
            for (var i = 0; i < points.GetLength(0); i++)
            {
                arrayOfPoints.Add(new BsonArray(2) { points[i, 0], points[i, 1] });
            }

            var condition = new BsonDocument("$within", new BsonDocument("$polygon", arrayOfPoints));
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Tests that the value of the named element is within a rectangle (see $within and $box).
        /// </summary>
        /// <param name="name">The name of the element to test.</param>
        /// <param name="lowerLeftX">The x coordinate of the lower left corner.</param>
        /// <param name="lowerLeftY">The y coordinate of the lower left corner.</param>
        /// <param name="upperRightX">The x coordinate of the upper right corner.</param>
        /// <param name="upperRightY">The y coordinate of the upper right corner.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery WithinRectangle(string name, double lowerLeftX, double lowerLeftY, double upperRightX, double upperRightY)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            var condition = new BsonDocument("$within", new BsonDocument("$box", new BsonArray { new BsonArray { lowerLeftX, lowerLeftY }, new BsonArray { upperRightX, upperRightY } }));
            return new QueryDocument(name, condition);
        }

        /// <summary>
        /// Generate a text search query that tests whether the given search string is present.
        /// </summary>
        /// <param name="searchString">The search string.</param>
        /// <returns>An IMongoQuery that represents the text search.</returns>
        public static ISequoiaQuery Text(string searchString)
        {
            return Text(searchString, null);
        }

        /// <summary>
        /// Generate a text search query that tests whether the given search string is present using the specified language's rules. 
        /// Specifies use of language appropriate stop words, stemming rules etc.
        /// </summary>
        /// <param name="searchString">The search string.</param>
        /// <param name="language">The language to restrict the search by.</param>
        /// <returns>An IMongoQuery that represents the text search for the particular language.</returns>
        public static ISequoiaQuery Text(string searchString, string language)
        {
            if (searchString == null)
            {
                throw new ArgumentNullException("searchString");
            }
            var condition = new BsonDocument
            {
                { "$search", searchString },
                { "$language", language, language != null }
            };
            return new QueryDocument("$text", condition);
        }

        // private methods
        private static void AddAndClause(BsonDocument query, BsonElement clause)
        {
            // flatten out nested $and
            if (clause.Name == "$and")
            {
                foreach (var item in clause.Value.AsBsonArray)
                {
                    foreach (var element in item.AsBsonDocument.Elements)
                    {
                        AddAndClause(query, element);
                    }
                }
                return;
            }

            if (query.ElementCount == 1 && query.GetElement(0).Name == "$and")
            {
                query[0].AsBsonArray.Add(new BsonDocument(clause));
            }
            else
            {
                if (clause.Name == "$and")
                {
                    PromoteQueryToDollarAndForm(query, clause);
                }
                else
                {
                    if (query.Contains(clause.Name))
                    {
                        var existingClause = query.GetElement(clause.Name);
                        if (existingClause.Value.IsBsonDocument && clause.Value.IsBsonDocument)
                        {
                            var clauseValue = clause.Value.AsBsonDocument;
                            var existingClauseValue = existingClause.Value.AsBsonDocument;
                            if (clauseValue.Names.Any(op => existingClauseValue.Contains(op)))
                            {
                                PromoteQueryToDollarAndForm(query, clause);
                            }
                            else
                            {
                                foreach (var element in clauseValue)
                                {
                                    existingClauseValue.Add(element);
                                }
                            }
                        }
                        else
                        {
                            PromoteQueryToDollarAndForm(query, clause);
                        }
                    }
                    else
                    {
                        query.Add(clause);
                    }
                }
            }
        }

        private static ISequoiaQuery NegateArbitraryQuery(BsonDocument query)
        {
            // $not only works as a meta operator on a single operator so simulate Not using $nor
            return new QueryDocument("$nor", new BsonArray { query });
        }

        private static ISequoiaQuery NegateQuery(BsonDocument query)
        {
            if (query.ElementCount == 1)
            {
                return NegateSingleElementQuery(query, query.GetElement(0));
            }
            else
            {
                return NegateArbitraryQuery(query);
            }
        }

        private static ISequoiaQuery NegateSingleElementQuery(BsonDocument query, BsonElement element)
        {
            if (element.Name[0] == '$')
            {
                return NegateSingleTopLevelOperatorQuery(query, element.Name, element.Value);
            }
            else
            {
                return NegateSingleFieldQuery(query, element.Name, element.Value);
            }
        }

        private static ISequoiaQuery NegateSingleFieldOperatorQuery(BsonDocument query, string fieldName, string operatorName, BsonValue args)
        {
            switch (operatorName)
            {
                case "$exists":
                    return new QueryDocument(fieldName, new BsonDocument("$exists", !args.AsBoolean));
                case "$in":
                    return new QueryDocument(fieldName, new BsonDocument("$nin", args.AsBsonArray));
                case "$ne":
                case "$not":
                    return new QueryDocument(fieldName, args);
                case "$nin":
                    return new QueryDocument(fieldName, new BsonDocument("$in", args.AsBsonArray));
                default:
                    return new QueryDocument(fieldName, new BsonDocument("$not", new BsonDocument(operatorName, args)));
            }
        }

        private static ISequoiaQuery NegateSingleFieldQuery(BsonDocument query, string fieldName, BsonValue selector)
        {
            var selectorDocument = selector as BsonDocument;
            if (selectorDocument != null)
            {
                if (selectorDocument.ElementCount >= 1)
                {
                    var operatorName = selectorDocument.GetElement(0).Name;
                    if (operatorName[0] == '$' && operatorName != "$ref")
                    {
                        if (selectorDocument.ElementCount == 1)
                        {
                            return NegateSingleFieldOperatorQuery(query, fieldName, operatorName, selectorDocument[0]);
                        }
                        else
                        {
                            return NegateArbitraryQuery(query);
                        }
                    }
                }
            }

            return NegateSingleFieldValueQuery(query, fieldName, selector);
        }

        private static ISequoiaQuery NegateSingleFieldValueQuery(BsonDocument query, string fieldName, BsonValue value)
        {
            if (value.IsBsonRegularExpression)
            {
                return new QueryDocument(fieldName, new BsonDocument("$not", value));
            }
            else
            {
                // turn implied equality comparison into $ne
                return new QueryDocument(fieldName, new BsonDocument("$ne", value));
            }
        }

        private static ISequoiaQuery NegateSingleTopLevelOperatorQuery(BsonDocument query, string operatorName, BsonValue args)
        {
            switch (operatorName)
            {
                case "$or":
                    return new QueryDocument("$nor", args);
                case "$nor":
                    return new QueryDocument("$or", args);
                default:
                    return NegateArbitraryQuery(query);
            }
        }

        private static void PromoteQueryToDollarAndForm(BsonDocument query, BsonElement clause)
        {
            var clauses = new BsonArray();
            foreach (var queryElement in query)
            {
                clauses.Add(new BsonDocument(queryElement));
            }
            clauses.Add(new BsonDocument(clause));
            query.Clear();
            query.Add("$and", clauses);
        }
    }

    /// <summary>
    /// Aids in building mongo queries based on type information.
    /// </summary>
    /// <typeparam name="TDocument">The type of the document.</typeparam>
    public static class Query<TDocument>
    {
        // public static methods
        /// <summary>
        /// Tests that the named array element contains all of the values (see $all).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery All<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            return new QueryBuilder<TDocument>().All(memberExpression, values);
        }

        /// <summary>
        /// Tests that at least one item of the named array element matches a query (see $elemMatch).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="elementQueryBuilderFunction">A function that builds a query using the supplied query builder.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery ElemMatch<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, Func<QueryBuilder<TValue>, ISequoiaQuery> elementQueryBuilderFunction)
        {
            return new QueryBuilder<TDocument>().ElemMatch(memberExpression, elementQueryBuilderFunction);
        }

        /// <summary>
        /// Tests that the value of the named element is equal to some value.
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery EQ<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().EQ(memberExpression, value);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is equal to some value.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery EQ<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().EQ(memberExpression, value);
        }

        /// <summary>
        /// Tests that an element of that name does or does not exist (see $exists).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Exists<TMember>(Expression<Func<TDocument, TMember>> memberExpression)
        {
            return new QueryBuilder<TDocument>().Exists(memberExpression);
        }        

        /// <summary>
        /// Tests that the value of the named element is greater than some value (see $gt).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GT<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().GT(memberExpression, value);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is greater than some value (see $lt).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GT<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().GT(memberExpression, value);
        }

        /// <summary>
        /// Tests that the value of the named element is greater than or equal to some value (see $gte).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GTE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().GTE(memberExpression, value);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is greater than or equal to some value (see $gte).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery GTE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().GTE(memberExpression, value);
        }

        /// <summary>
        /// Tests that the value of the named element is equal to one of a list of values (see $in).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery In<TMember>(Expression<Func<TDocument, TMember>> memberExpression, IEnumerable<TMember> values)
        {
            return new QueryBuilder<TDocument>().In(memberExpression, values);
        }

        /// <summary>
        /// Tests that any of the values in the named array element are equal to one of a list of values (see $in).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery In<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            return new QueryBuilder<TDocument>().In(memberExpression, values);
        }

        /// <summary>
        /// Tests that the value of the named element is less than some value (see $lt).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LT<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().LT(memberExpression, value);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is less than some value (see $lt).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LT<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().LT(memberExpression, value);
        }

        /// <summary>
        /// Tests that the value of the named element is less than or equal to some value (see $lte).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LTE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().LTE(memberExpression, value);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is less than or equal to some value (see $lte).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery LTE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().LTE(memberExpression, value);
        }

        /// <summary>
        /// Tests that the value of the named element matches a regular expression (see $regex).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="regex">The regex.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Matches(Expression<Func<TDocument, string>> memberExpression, BsonRegularExpression regex)
        {
            return new QueryBuilder<TDocument>().Matches(memberExpression, regex);
        }

        /// <summary>
        /// Tests that any of the values in the named array element matches a regular expression (see $regex).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="regex">The regex.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Matches(Expression<Func<TDocument, IEnumerable<string>>> memberExpression, BsonRegularExpression regex)
        {
            return new QueryBuilder<TDocument>().Matches(memberExpression, regex);
        }

        /// <summary>
        /// Tests that the modulus of the value of the named element matches some value (see $mod).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="modulus">The modulus.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Mod(Expression<Func<TDocument, int>> memberExpression, long modulus, long value)
        {
            return new QueryBuilder<TDocument>().Mod(memberExpression, modulus, value);
        }

        /// <summary>
        /// Tests that the any of the values in the named array element match some value (see $mod).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="modulus">The modulus.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Mod(Expression<Func<TDocument, IEnumerable<int>>> memberExpression, long modulus, long value)
        {
            return new QueryBuilder<TDocument>().Mod(memberExpression, modulus, value);
        }

        /// <summary>
        /// Tests that an element does not equal the value (see $ne).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            return new QueryBuilder<TDocument>().NE(memberExpression, value);
        }

        /// <summary>
        /// Tests that none of the values in the named array element is equal to some value (see $ne).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            return new QueryBuilder<TDocument>().NE(memberExpression, value);
        }

        /// <summary>
        /// Tests that an element of that name does not exist (see $exists).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NotExists<TMember>(Expression<Func<TDocument, TMember>> memberExpression)
        {
            return new QueryBuilder<TDocument>().NotExists(memberExpression);
        }

        /// <summary>
        /// Tests that the value of the named element is not equal to any item in a list of values (see $nin).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NotIn<TMember>(Expression<Func<TDocument, TMember>> memberExpression, IEnumerable<TMember> values)
        {
            return new QueryBuilder<TDocument>().NotIn(memberExpression, values);
        }

        /// <summary>
        /// Tests that the none of the values of the named array element is equal to any item in a list of values (see $nin).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="values">The values to compare.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery NotIn<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            return new QueryBuilder<TDocument>().NotIn(memberExpression, values);
        }

        /// <summary>
        /// Tests that the size of the named array is equal to some value (see $size).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Size<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, int size)
        {
            return new QueryBuilder<TDocument>().Size(memberExpression, size);
        }

        /// <summary>
        /// Tests that the type of the named element is equal to some type (see $type).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="type">The type to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Type<TMember>(Expression<Func<TDocument, TMember>> memberExpression, BsonType type)
        {
            return new QueryBuilder<TDocument>().Type(memberExpression, type);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is equal to some type (see $type).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="type">The type to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Type<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, BsonType type)
        {
            return new QueryBuilder<TDocument>().Type(memberExpression, type);
        }

        /// <summary>
        /// Builds a query from an expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns>An IMongoQuery.</returns>
        public static ISequoiaQuery Where(Expression<Func<TDocument, bool>> expression)
        {
            return new QueryBuilder<TDocument>().Where(expression);
        }
    }

    /// <summary>
    /// Aids in building mongo queries based on type information.
    /// </summary>
    /// <typeparam name="TDocument">The type of the document.</typeparam>
    public class QueryBuilder<TDocument>
    {
        // private fields
        private readonly BsonSerializationInfoHelper _serializationInfoHelper;
        private readonly PredicateTranslator _predicateTranslator;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBuilder{TDocument}"/> class.
        /// </summary>
        public QueryBuilder()
            : this(new BsonSerializationInfoHelper())
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBuilder{TDocument}"/> class.
        /// </summary>
        /// <param name="serializationInfoHelper">The serialization info helper.</param>
        internal QueryBuilder(BsonSerializationInfoHelper serializationInfoHelper)
        {
            _serializationInfoHelper = serializationInfoHelper;
            _predicateTranslator = new PredicateTranslator(_serializationInfoHelper);
        }

        // public methods
        /// <summary>
        /// Tests that the named array element contains all of the values (see $all).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery All<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("All", serializationInfo);
            var serializedValues = _serializationInfoHelper.SerializeValues(itemSerializationInfo, values);
            return Query.All(serializationInfo.ElementName, serializedValues);
        }

        /// <summary>
        /// Tests that all the queries are true (see $and in newer versions of the server).
        /// </summary>
        /// <param name="queries">A list of subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery And(IEnumerable<ISequoiaQuery> queries)
        {
            return Query.And(queries);
        }

        /// <summary>
        /// Tests that all the queries are true (see $and in newer versions of the server).
        /// </summary>
        /// <param name="queries">A list of subqueries.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery And(params ISequoiaQuery[] queries)
        {
            return And((IEnumerable<ISequoiaQuery>)queries);
        }

        /// <summary>
        /// Tests that at least one item of the named array element matches a query (see $elemMatch).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="elementQueryBuilderFunction">A function that builds a query using the supplied query builder.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery ElemMatch<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, Func<QueryBuilder<TValue>, ISequoiaQuery> elementQueryBuilderFunction)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (elementQueryBuilderFunction == null)
            {
                throw new ArgumentNullException("elementQueryBuilderFunction");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("ElemMatch", serializationInfo);
            var elementQueryBuilder = new QueryBuilder<TValue>(_serializationInfoHelper);
            var elementQuery = elementQueryBuilderFunction(elementQueryBuilder);
            return Query.ElemMatch(serializationInfo.ElementName, elementQuery);
        }

        /// <summary>
        /// Tests that the value of the named element is equal to some value.
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery EQ<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.EQ(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is equal to some value.
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery EQ<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("EQ", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.EQ(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that an element of that name does or does not exist (see $exists).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Exists<TMember>(Expression<Func<TDocument, TMember>> memberExpression)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Exists(serializationInfo.ElementName);
        }

        /// <summary>
        /// Tests that the value of the named element is greater than some value (see $gt).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery GT<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.GT(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is greater than some value (see $lt).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery GT<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("GT", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.GT(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that the value of the named element is greater than or equal to some value (see $gte).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery GTE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.GTE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is greater than or equal to some value (see $gte).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery GTE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("GTE", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.GTE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that the value of the named element is equal to one of a list of values (see $in).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery In<TMember>(Expression<Func<TDocument, TMember>> memberExpression, IEnumerable<TMember> values)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValues = _serializationInfoHelper.SerializeValues(serializationInfo, values);
            return Query.In(serializationInfo.ElementName, serializedValues);
        }

        /// <summary>
        /// Tests that any of the values in the named array element are equal to one of a list of values (see $in).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery In<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("In", serializationInfo);
            var serializedValues = _serializationInfoHelper.SerializeValues(itemSerializationInfo, values);
            return Query.In(serializationInfo.ElementName, serializedValues);
        }

        /// <summary>
        /// Tests that the value of the named element is less than some value (see $lt).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery LT<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.LT(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is less than some value (see $lt).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery LT<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("LT", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.LT(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that the value of the named element is less than or equal to some value (see $lte).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery LTE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.LTE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is less than or equal to some value (see $lte).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery LTE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("LTE", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.LTE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that the value of the named element matches a regular expression (see $regex).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="regex">The regex.</param>
        /// <returns>
        /// A query.
        /// </returns>
        public ISequoiaQuery Matches(Expression<Func<TDocument, string>> memberExpression, BsonRegularExpression regex)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (regex == null)
            {
                throw new ArgumentNullException("regex");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Matches(serializationInfo.ElementName, regex);
        }

        /// <summary>
        /// Tests that any of the values in the named array element matches a regular expression (see $regex).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="regex">The regex.</param>
        /// <returns>
        /// A query.
        /// </returns>
        public ISequoiaQuery Matches(Expression<Func<TDocument, IEnumerable<string>>> memberExpression, BsonRegularExpression regex)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (regex == null)
            {
                throw new ArgumentNullException("regex");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Matches(serializationInfo.ElementName, regex);
        }

        /// <summary>
        /// Tests that the modulus of the value of the named element matches some value (see $mod).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="modulus">The modulus.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Mod(Expression<Func<TDocument, int>> memberExpression, long modulus, long value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Mod(serializationInfo.ElementName, modulus, value);
        }

        /// <summary>
        /// Tests that the any of the values in the named array element match some value (see $mod).
        /// </summary>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="modulus">The modulus.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Mod(Expression<Func<TDocument, IEnumerable<int>>> memberExpression, long modulus, long value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Mod(serializationInfo.ElementName, modulus, value);
        }
        
        /// <summary>
        /// Tests that the inverse of the query is true (see $not).
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Not(ISequoiaQuery query)
        {
            return Query.Not(query);
        }

        /// <summary>
        /// Tests that an element does not equal the value (see $ne).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="value">The value.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery NE<TMember>(Expression<Func<TDocument, TMember>> memberExpression, TMember value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValue = _serializationInfoHelper.SerializeValue(serializationInfo, value);
            return Query.NE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that none of the values in the named array element is equal to some value (see $ne).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="value">The value to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery NE<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, TValue value)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("NE", serializationInfo);
            var serializedValue = _serializationInfoHelper.SerializeValue(itemSerializationInfo, value);
            return Query.NE(serializationInfo.ElementName, serializedValue);
        }

        /// <summary>
        /// Tests that an element of that name does not exist (see $exists).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery NotExists<TMember>(Expression<Func<TDocument, TMember>> memberExpression)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.NotExists(serializationInfo.ElementName);
        }

        /// <summary>
        /// Tests that the value of the named element is not equal to any item in a list of values (see $nin).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="values">The values to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery NotIn<TMember>(Expression<Func<TDocument, TMember>> memberExpression, IEnumerable<TMember> values)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var serializedValues = _serializationInfoHelper.SerializeValues(serializationInfo, values);
            return Query.NotIn(serializationInfo.ElementName, serializedValues);
        }

        /// <summary>
        /// Tests that the none of the values of the named array element is equal to any item in a list of values (see $nin).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="values">The values to compare.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery NotIn<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, IEnumerable<TValue> values)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }
            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            var itemSerializationInfo = _serializationInfoHelper.GetItemSerializationInfo("NotIn", serializationInfo);
            var serializedValues = _serializationInfoHelper.SerializeValues(itemSerializationInfo, values);
            return Query.NotIn(serializationInfo.ElementName, serializedValues);
        }

        /// <summary>
        /// Tests that at least one of the subqueries is true (see $or).
        /// </summary>
        /// <param name="queries">The subqueries.</param>
        /// <returns>
        /// A query.
        /// </returns>
        public ISequoiaQuery Or(IEnumerable<ISequoiaQuery> queries)
        {
            return Query.Or(queries);
        }

        /// <summary>
        /// Tests that at least one of the subqueries is true (see $or).
        /// </summary>
        /// <param name="queries">The subqueries.</param>
        /// <returns>
        /// A query.
        /// </returns>
        public ISequoiaQuery Or(params ISequoiaQuery[] queries)
        {
            return Or((IEnumerable<ISequoiaQuery>)queries);
        }

        /// <summary>
        /// Tests that the size of the named array is equal to some value (see $size).
        /// </summary>
        /// <typeparam name="TValue">The type of the enumerable member values.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="size">The size to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Size<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, int size)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Size(serializationInfo.ElementName, size);
        }

        /// <summary>
        /// Tests that the type of the named element is equal to some type (see $type).
        /// </summary>
        /// <typeparam name="TMember">The member type.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="type">The type to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Type<TMember>(Expression<Func<TDocument, TMember>> memberExpression, BsonType type)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Type(serializationInfo.ElementName, type);
        }

        /// <summary>
        /// Tests that any of the values in the named array element is equal to some type (see $type).
        /// </summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="memberExpression">The member expression representing the element to test.</param>
        /// <param name="type">The type to compare to.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Type<TValue>(Expression<Func<TDocument, IEnumerable<TValue>>> memberExpression, BsonType type)
        {
            if (memberExpression == null)
            {
                throw new ArgumentNullException("memberExpression");
            }

            var serializationInfo = _serializationInfoHelper.GetSerializationInfo(memberExpression);
            return Query.Type(serializationInfo.ElementName, type);
        }

        /// <summary>
        /// Builds a query from an expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns>An IMongoQuery.</returns>
        public ISequoiaQuery Where(Expression<Func<TDocument, bool>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            var evaluatedExpression = PartialEvaluator.Evaluate(expression.Body);
            return _predicateTranslator.BuildQuery(evaluatedExpression);
        }
    }
}
