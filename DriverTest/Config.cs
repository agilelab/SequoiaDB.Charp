using System;
using System.IO;
using System.Xml.Serialization;

namespace DriverTest
{
    public class Sdb
    {
        public string Address { get; set; }
    }

    public class Node
    {
        public string HostName { get; set; }
        public int Port { get; set; }
        public string DBPath { get; set; }
        public string DiagLevel { get; set; }
    }

    public class Group
    {
        public string GroupName { get; set; }
        [XmlArray]
        public Node[] Nodes;
    }

    [XmlRootAttribute("TestConfig")]
    public class TestConfig
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public Sdb Coord;
        public Sdb Catalog;
        public Sdb Data;
        [XmlArray]
        public Group[] Groups;
    }

    public class Config
    {
        public TestConfig conf;
        public Config()
        {
            XmlSerializer serializer = new XmlSerializer(typeof(TestConfig));
            FileStream fs = new FileStream("Config.xml", FileMode.Open);
            conf = (TestConfig)serializer.Deserialize(fs);
            fs.Close();
        }
    }
}
