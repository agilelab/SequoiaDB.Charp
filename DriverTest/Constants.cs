using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SequoiaDB;
using SequoiaDB.Bson;
using SequoiaDB.Driver;

namespace DriverTest
{
    class Constants
    {
        public static Boolean isClusterEnv(Sequoiadb sdb)
        {
            try
            {
                sdb.ListReplicaGroups();
            }
            catch (BaseException e)
            {
                int errcode = e.ErrorCode;
                if (new BaseException("SDB_RTN_COORD_ONLY").ErrorCode == errcode)
                    return false;
                else
                    throw e;
            }
            catch (System.Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Environment.Exit(0);
            }
            return true;
        }
    }
}
