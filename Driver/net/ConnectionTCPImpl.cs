using System.IO;
using System.Net;
using System.Net.Sockets;
using System;
using System.Threading;

namespace SequoiaDB.Driver
{
    internal class ConnectionTCPImpl : IConnection
    {
        private TcpClient connection;
        private NetworkStream input, output;
        private ConfigOptions options;
        private ServerAddress hostAddress;

        //private readonly Logger logger = new Logger("IConnectionICPImpl");

        public ConnectionTCPImpl(ServerAddress addr, ConfigOptions opts)
        {
            this.hostAddress = addr;
            this.options = opts;
        }

        public void Connect()
        {
            if (connection != null)
                return;
            TimeSpan sleepTime = new TimeSpan(0,0,0,0,100);
            TimeSpan maxAutoConnectRetryTime = new TimeSpan(0, 0, 0, 0, options.MaxAutoConnectRetryTime);
            DateTime start = DateTime.Now;

            while (true)
            {
                IOException lastError = null;
                IPEndPoint addr = hostAddress.HostAddress;
                try 
                {
                    connection = new TimeOutSocket().Connect(addr, options.ConnectTimeout);
                    connection.NoDelay = (!options.UseNagle);
                    connection.SendTimeout = options.SendTimeout;
                    connection.ReceiveTimeout = options.ReceiveTimeout;
                    input = connection.GetStream();
                    output = connection.GetStream();
                    return;
                }
                catch(Exception e)
                {
                    lastError = new IOException("Couldn't connect to ["
                            + addr.ToString() + "] exception:" + e);
                    Close();
                }

                TimeSpan executeTime = DateTime.Now - start;
                if (executeTime >= maxAutoConnectRetryTime)
                    throw lastError;
                if (sleepTime + executeTime > maxAutoConnectRetryTime)
                    sleepTime = maxAutoConnectRetryTime - executeTime;
                try
                {
                    Thread.Sleep(sleepTime);
                }
                catch (Exception){}
                sleepTime = sleepTime + sleepTime;
            }
        }

        public void Close()
        {
            if (connection !=null && connection.Connected)
            {
                input.Close();
                output.Close();
                connection.Close();
            }
		    connection = null;
        }

        public bool IsClosed()
        {
            if ( connection == null || !connection.Connected )
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void ChangeConfigOptions(ConfigOptions opts)
        {
            this.options = opts;
            Close();
            Connect();
        }

        public void SendMessage(byte[] msg)
        {
            try
            {
                if ( null != connection )
                {
			        output.Write(msg, 0, msg.Length);
		        }
            }
            catch (IOException e)
            {
                throw e;
            }
            catch (System.Exception)
            {
                throw new BaseException("SDB_NET_SEND_ERR");
            }
        }

        public byte[] ReceiveMessage(bool isBigEndian)
        {
            try
            {
                byte[] buf = new byte[4];
                int rtn = 0;
                int retSize = 0;
                // get the total length of the message
                while (rtn < 4)
                {
                    try
                    {
                        retSize = input.Read(buf, rtn, 4 - rtn);
                    }
                    catch (System.Exception)
                    {
                        Close();
                        throw new IOException("Expect 4-byte message length");
                    }
                    rtn += retSize;
                }
                int msgSize = Helper.ByteToInt(buf, isBigEndian);
                // get the rest part of message
                byte[] rtnBuf = new byte[msgSize];
                Array.Copy(buf, rtnBuf, 4);
                rtn = 4;
                retSize = 0;
                while (rtn < msgSize)
                {
                    try
                    {
                        retSize = input.Read(rtnBuf, rtn, msgSize - rtn);
                    }
                    catch (System.Exception)
                    {
                        Close();
                        throw new IOException("Failed to read from socket");
                    }
                    rtn += retSize;
                }
                // check
                if (rtn != msgSize)
                {
                    Close();
                    throw new IOException("Message length in header incorrect");
                }
                return rtnBuf;
            }
            catch (IOException e)
            {
                throw e;
            }
            catch (System.Exception)
            {
                throw new BaseException("SDB_NETWORK");
            }
        }

        public byte[] ReceiveMessage(int msgSize)
        {
            byte[] rtnBuf = new byte[msgSize];
            int rtn = 0;
            int retSize = 0;
            while (rtn < msgSize)
            {
                retSize = input.Read(rtnBuf, rtn, msgSize - rtn);
                if (-1 == retSize)
                {
                    Close();
                    throw new IOException("Failed to read from socket");
                }
                rtn += retSize;
            }

            if (rtn != msgSize)
            {
                Close();
                throw new IOException("Message length in header incorrect");
            }

            return rtnBuf; 
        }
    }
}
