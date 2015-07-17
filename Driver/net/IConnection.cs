namespace SequoiaDB.Driver
{
    public interface IConnection
    {
        void Connect();
        void Close();
        bool IsClosed();

        void ChangeConfigOptions(ConfigOptions opts);

        void SendMessage(byte[] msg);
        byte[] ReceiveMessage(bool isBigEndian);
        byte[] ReceiveMessage(int msgSize);
    }
}
