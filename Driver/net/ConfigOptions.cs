/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class ConfigOptions
     *  \brief Database Connection Configuration Option
     */
    public class ConfigOptions
    {
        // seconds
        private int maxAutoConnectRetryTime = 15;
        // milliseconds
        private int connectTimeout = 10000;
        // Send and Reive Timeout: default is no timeout
        private int sendTimeout = 0;
        private int receiveTimeout = 0;

        private bool useNagle = false;

        /** \property MaxAutoConnectRetryTime
         *  \brief Get or group the max autoconnect retry time(seconds)
         */
        public int MaxAutoConnectRetryTime
        {
            get
            {
                return maxAutoConnectRetryTime;
            }
            set
            {
                maxAutoConnectRetryTime = value;
            }
        }

        /** \property ConnectTimeout
         *  \brief Get or group the connect timeout(milliseconds)
         */
        public int ConnectTimeout
        {
            get
            {
                return connectTimeout;
            }
            set
            {
                connectTimeout = value;
            }
        }

        /** \property SendTimeout
         *  \brief Get or group the send timeout(milliseconds)
         */
        public int SendTimeout
        {
            get
            {
                return sendTimeout;
            }
            set
            {
                sendTimeout = value;
            }
        }

        /** \property ReceiveTimeout
         *  \brief Get or group the receive timeout(milliseconds)
         */
        public int ReceiveTimeout
        {
            get
            {
                return receiveTimeout;
            }
            set
            {
                receiveTimeout = value;
            }
        }

        /** \property UseNagle
         *  \brief Get or group the Nagle Algorithm
         */
        public bool UseNagle
        {
            get
            {
                return useNagle;
            }
            set
            {
                useNagle = value;
            }
        }
    }
}
