using SequoiaDB.Bson;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class DBQuery
     *  \brief Database operation rules
     */
    public class DBQuery
   {
        private long skipRowsCount = 0;
        private long returnRowsCount = -1;
        private int flag = 0;

	    /** \memberof FLG_QUERY_STRINGOUT 0x00000001
	     *  \brief Normally, query return bson stream, 
	     *         when this flag is added, query return binary data stream
	     */
	    public const int FLG_QUERY_STRINGOUT = 0x00000001;
	
	    /** \memberof FLG_INSERT_CONTONDUP 0x00000080
	     *  \brief Force to use specified hint to query,
	     *         if database have no index assigned by the hint, fail to query
	     */
	    public const int FLG_QUERY_FORCE_HINT = 0x00000080;
	
	    /** \memberof FLG_QUERY_PARALLED 0x00000100
	     *  \brief Enable paralled sub query
	     */
	    public const int FLG_QUERY_PARALLED = 0x00000100;
	
	    /** \memberof FLG_QUERY_WITH_RETURNDATA 0x00000200
         *  \brief Return data in query response
         */
	    public const int FLG_QUERY_WITH_RETURNDATA = 0x00000200;
	
	    /** \memberof FLG_QUERY_EXPLAIN 0x00000400
	     *  \brief Explain query
	     */
        public const int FLG_QUERY_EXPLAIN = 0x00000400;

       /** \property Matcher
        *  \brief Matching rule
        */
        public BsonDocument Matcher { get; set; }

        /** \property Selector
         *  \brief selective rule
         */
        public BsonDocument Selector { get; set; }

        /** \property OrderBy
         *  \brief Ordered rule
         */
        public BsonDocument OrderBy { get; set; }

        /** \property Hint
         *  \brief Sepecified access plan
         */
        public BsonDocument Hint { get; set; }

        /** \property Modifier
         *  \brief Modified rule
         */
        public BsonDocument Modifier { get; set; }

        /** \property SkipRowsCount
         *  \brief Documents to skip
         */
        public long SkipRowsCount
        {
            get { return skipRowsCount; }
            set { skipRowsCount = value; }
        }

        /** \property ReturnRowsCount
         *  \brief Documents to return
         */
        public long ReturnRowsCount
        {
            get { return returnRowsCount; }
            set { returnRowsCount = value; }
        }

        /** \property Flag
         *  \brief Query flag
         */
        public int Flag
        {
            get { return flag; }
            set { flag = value; }
        }

   }
}
