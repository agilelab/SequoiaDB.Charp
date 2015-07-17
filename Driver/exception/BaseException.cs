using System;
using SequoiaDB.Driver;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class BaseException
     *  \brief Database operation exception
     */
    public class BaseException : Exception
    {
        private const long serialVersionUID = -6115487863398926195L;

        private string message;
        private string errorType;
        private int errorCode;

        internal BaseException(int errorCode)
        {
            try 
            {
                this.message = SDBErrorLookup.GetErrorDescriptionByCode(errorCode);
                this.errorType = SDBErrorLookup.GetErrorTypeByCode(errorCode);
                this.errorCode = errorCode;
            }
            catch (Exception)
            {                
                this.message = SequoiadbConstants.UNKONWN_DESC;
                this.errorType = SequoiadbConstants.UNKNOWN_TYPE;
                this.errorCode = SequoiadbConstants.UNKNOWN_CODE;
            }
        }

        internal BaseException(string errorType)
        {
            try
            {
                this.message = SDBErrorLookup.GetErrorDescriptionByType(errorType);
                this.errorCode = SDBErrorLookup.GetErrorCodeByType(errorType);
                this.errorType = errorType;
            }
            catch (Exception)
            {
                this.message = SequoiadbConstants.UNKONWN_DESC;
                this.errorType = SequoiadbConstants.UNKNOWN_TYPE;
                this.errorCode = SequoiadbConstants.UNKNOWN_CODE;
            }
        }

        /** \property Message
         *  \brief Get the error description of exception
         */
        public override string Message
        {
            get
            {
                return this.message;
            }
        }

        /** \property ErrorType
         *  \brief Get the error type of exception
         */
        public string ErrorType
        {
            get 
            {
                return this.errorType;
            }
        }

        /** \property ErrorCode
         *  \brief Get the error code of exception
         */
        public int ErrorCode
        {
            get
            {
                return this.errorCode;
            }
        }
    }
}
