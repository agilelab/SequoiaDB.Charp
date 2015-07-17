using SequoiaDB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using SequoiaDB.Driver;

namespace DriverTest
{
    
    
    /// <summary>
    ///这是 BaseExceptionTest 的测试类，旨在
    ///包含所有 BaseExceptionTest 单元测试
    ///</summary>
    [TestClass()]
    public class BaseExceptionTest
    {


        private TestContext testContextInstance;

        /// <summary>
        ///获取或设置测试上下文，上下文提供
        ///有关当前测试运行及其功能的信息。
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }


        [TestMethod()]
        public void ExceptionTest()
        {
            string errorType = "SDB_DMS_EOC";
            BaseException ex = new BaseException(errorType);
            Assert.IsTrue(ex.Message.ToString().Equals("End of collection"));
        }
    }
}
