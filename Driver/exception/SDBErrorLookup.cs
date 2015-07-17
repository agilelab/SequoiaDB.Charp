using System;
using System.Collections.Generic;
using System.IO;
using SequoiaDB.Driver;

namespace SequoiaDB.Driver
{
    class SDBErrorLookup
    {
        private static Dictionary<string, SDBError> mapByType = null;
        private static Dictionary<int, SDBError> mapByCode = null;

        private static void LoadErrorMap()
        {
            mapByType = new Dictionary<string, SDBError>();
            mapByCode = new Dictionary<int, SDBError>();

            foreach (int errCode in Enum.GetValues(typeof(Errors.errors)))
            {
                SDBError error = new SDBError();
                string errType = Enum.GetName(typeof(Errors.errors), errCode);
                error.ErrorCode = errCode;
                error.ErrorType = errType;
                error.ErrorDescription = Errors.descriptions[(-errCode) - 1];

                mapByCode.Add(errCode, error);
                mapByType.Add(errType, error);
            }
        }

        public static string GetErrorDescriptionByType(string errorType)
        {
            if (mapByType == null)
                LoadErrorMap();
            SDBError errObj = mapByType[errorType];
            if (errObj != null)
                return errObj.ErrorDescription;
            return SequoiadbConstants.UNKONWN_DESC;
        }

        public static string GetErrorDescriptionByCode(int errorCode)
        {
            if (mapByType == null)
                LoadErrorMap();
            SDBError errObj = mapByCode[errorCode];
            if (errObj != null)
                return errObj.ErrorDescription;
            return SequoiadbConstants.UNKONWN_DESC;
        }

        public static int GetErrorCodeByType(string errorType)
        {
            if (mapByType == null)
                LoadErrorMap();
            SDBError errObj = mapByType[errorType];
            if (errObj != null)
                return errObj.ErrorCode;
            return SequoiadbConstants.UNKNOWN_CODE;
        }

        public static string GetErrorTypeByCode(int errorCode)
        {
            if (mapByType == null)
                LoadErrorMap();
            SDBError errObj = mapByCode[errorCode];
            if (errObj != null)
                return errObj.ErrorType;
            return SequoiadbConstants.UNKNOWN_TYPE;
        }
    }
}
