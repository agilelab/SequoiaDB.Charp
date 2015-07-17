using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System;
using SequoiaDB.Bson;
using System.IO;

/** \namespace SequoiaDB
 *  \brief SequoiaDB Driver for C#.Net
 *  \author Hetiu Lin
 */
namespace SequoiaDB.Driver
{
    /** \class Sequoiadb
     *  \brief Database operation interfaces of admin
     */
    public class SequoiaClient : Sequoiadb
    {
        public SequoiaClient():base()
        {
        }

        public SequoiaClient(string connString):base(connString)
        {
            
        }

        public SequoiaClient(List<string> connStrings):base()
        {
            
        }

        public SequoiaClient(string host, int port):base(host,port)
        {
            
        }
    }
}
