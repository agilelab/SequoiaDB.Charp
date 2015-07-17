using System;
using System.IO;

namespace SequoiaDB.Driver
{
    public class Logger
    {
        private string name;
        private bool isDebugEnabled = false;
        public bool IsDebugEnabled
        {
            get { return isDebugEnabled; }
            set { isDebugEnabled = value; }
        }

        private string fileName;
        private FileStream fs;
        private StreamWriter sw;

        public Logger(string name)
        {
            this.name = name;
            fileName = DateTime.Now.ToLongDateString() + ".log";
        }

        public void Debug(string msg)
        {
            fs = new FileStream(fileName, FileMode.Append);
            sw = new StreamWriter(fs);
            sw.Write(name + " ====> ");
            sw.WriteLine("{0} {1}", DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString());
            sw.WriteLine("<===== Debug =====>");
            sw.WriteLine(msg);
            sw.Flush();
            sw.Close();
            fs.Close();
        }

        public void Error(string msg, Exception e)
        {
            fs = new FileStream(fileName, FileMode.Append);
            sw = new StreamWriter(fs);
            sw.Write(name + " ====> ");
            sw.WriteLine("{0} {1}", DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString());
            sw.WriteLine("<===== Error =====>");
            sw.WriteLine(msg);
            sw.WriteLine(e.ToString());
            sw.Flush();
            sw.Close();
            fs.Close();
        }

        public void Error(string msg)
        {
            fs = new FileStream(fileName, FileMode.Append);
            sw = new StreamWriter(fs);
            sw.Write(name + " ====> ");
            sw.WriteLine("{0} {1}", DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString());
            sw.WriteLine("<===== Error =====>");
            sw.WriteLine(msg);
            sw.Flush();
            sw.Close();
            fs.Close();
        }

        public void Info(string msg)
        {
            fs = new FileStream(fileName, FileMode.Append);
            sw = new StreamWriter(fs);
            sw.Write(name + " ====> ");
            sw.WriteLine("{0} {1}", DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString());
            sw.WriteLine("<===== Information =====>");
            sw.WriteLine(msg);
            sw.Flush();
            sw.Close();
            fs.Close();
        }
    }
}
