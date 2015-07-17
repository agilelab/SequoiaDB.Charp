namespace SequoiaDB.Driver
{
	public class SDBConst
	{
        /** \memberof FLG_INSERT_CONTONDUP 0x00000001
         *   \brief The flags represent whether bulk insert continue when hitting index key duplicate error
         */
        public const int FLG_INSERT_CONTONDUP = 0x00000001;

        public const int SDB_PAGESIZE_4K = 4096;
        public const int SDB_PAGESIZE_8K = 8192;
        public const int SDB_PAGESIZE_16K = 16384;
        public const int SDB_PAGESIZE_32K = 32768;
        public const int SDB_PAGESIZE_64K = 65536;
        public const int SDB_PAGESIZE_DEFAULT = 0;

        public const int SDB_SNAP_CONTEXTS         = 0;
        public const int SDB_SNAP_CONTEXTS_CURRENT = 1;
        public const int SDB_SNAP_SESSIONS         = 2;
        public const int SDB_SNAP_SESSIONS_CURRENT = 3;
        public const int SDB_SNAP_COLLECTIONS      = 4;
        public const int SDB_SNAP_COLLECTIONSPACES = 5;
        public const int SDB_SNAP_DATABASE         = 6;
        public const int SDB_SNAP_SYSTEM           = 7;
        public const int SDB_SNAP_CATALOG          = 8;

        public const int SDB_LIST_CONTEXTS         = 0;
        public const int SDB_LIST_CONTEXTS_CURRENT = 1;
        public const int SDB_LIST_SESSIONS         = 2;
        public const int SDB_LIST_SESSIONS_CURRENT = 3;
        public const int SDB_LIST_COLLECTIONS      = 4;
        public const int SDB_LIST_COLLECTIONSPACES = 5;
        public const int SDB_LIST_STORAGEUNITS     = 6;
        public const int SDB_LIST_GROUPS           = 7;
        public const int SDB_LIST_STOREPROCEDURES  = 8;
        public const int SDB_LIST_DOMAINS          = 9;
        public const int SDB_LIST_TASKS            = 10;
        public const int SDB_LIST_CS_IN_DOMAIN     = 11;
        public const int SDB_LIST_CL_IN_DOMAIN     = 12;

        public enum NodeStatus
        {
            SDB_NODE_ALL = 0,
            SDB_NODE_ACTIVE,
            SDB_NODE_INACTIVE,
            SDB_NODE_UNKNOWN
        };
	}
}
