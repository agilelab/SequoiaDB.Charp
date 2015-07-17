using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace SequoiaDB.Driver
{
    class TimeOutSocket
    {
        private TcpClient client;
        private ManualResetEvent timeoutObject;
        private Exception socketException = new BaseException("SDB_NET_CANNOT_CONNECT");

        //private static readonly Logger logger = new Logger("TimeOutSocket2");

        internal TimeOutSocket()
        {
            this.client = new TcpClient();
            this.timeoutObject = new ManualResetEvent(false);
        }

        internal TcpClient Connect(IPEndPoint remoteEndPoint, int timeoutMSec)
        {
            string servIP = Convert.ToString(remoteEndPoint.Address);
            int servPort = remoteEndPoint.Port;
            try
            {
                client.BeginConnect(servIP, servPort, new AsyncCallback(CallBackMethod), client);
            }
            catch (Exception)
            {
                throw new BaseException("SDB_NET_CANNOT_CONNECT");
            }
            timeoutObject.Reset();
            if (timeoutObject.WaitOne(timeoutMSec, false))
            {
                if (client.Connected)
                {
                    return client;
                }
                else
                {
                    client.Close();
                    throw socketException;
                }
            }
            else
            {
                client.Close();
                throw new BaseException("SDB_TIMEOUT");
            }
        }

        private void CallBackMethod(IAsyncResult asyncResult)
        {
            try
            {
                TcpClient client = asyncResult.AsyncState as TcpClient;
                if (client != null && client.Client != null)
                {
                    client.EndConnect(asyncResult);
                }
                else
                {
                    throw new BaseException("SDB_NET_CANNOT_CONNECT");
                }
            }
            catch (Exception)
            {
                socketException = new BaseException("SDB_NET_CANNOT_CONNECT");
            }
            finally
            {
                timeoutObject.Set();
            }
        }
    }
}
