using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using EAS.Data;
using EAS.Data.Access;
using EAS.Data.ORM;
using EAS.Data.Linq;

namespace AgileHIS.Entities
{
    /// <summary>
    /// 数据上下文。
    /// </summary>
    class DbEntities : DataContext
    {
        #region 字段定义

        private DataEntityQuery<GBCode> m_GBCodes;
        private DataEntityQuery<HFareDetail> m_HFareDetails;
        #endregion

        #region 构造函数

        /// <summary>
        /// 初始化DbEntities对象实例。
        /// </summary>
        public DbEntities()
        {
        }

        /// <summary>
        /// 初始化DbEntities对象实例。
        /// </summary>
        /// <param name="dbProvider">数据库访问程序提供者。</param>
        public DbEntities(IDbProvider dbProvider)
            : base(dbProvider)
        {

        }

        /// <summary>
        /// 初始化DbEntities对象实例。
        /// </summary>
        /// <param name="dataAccessor">数据访问器。</param>
        public DbEntities(IDataAccessor dataAccessor)
            : base(dataAccessor)
        {

        }

        /// <summary>
        /// 初始化DbEntities对象实例。
        /// </summary>
        /// <param name="dataAccessor">数据访问器。</param>
        /// <param name="ormAccessor">Orm访问器。</param>
        public DbEntities(IDataAccessor dataAccessor, IOrmAccessor ormAccessor)
            : base(dataAccessor, ormAccessor)
        {

        }

        #endregion

        #region 查询定义

        /// <summary>
        /// 国标码。
        /// </summary>
        public DataEntityQuery<GBCode> GBCodes
        {
            get
            {
                if (this.m_GBCodes == null)
                {
                    this.m_GBCodes = base.CreateQuery<GBCode>();
                }
                return this.m_GBCodes;
            }
        }

        /// <summary>
        /// 住院费用明细记录。
        /// </summary>
        public DataEntityQuery<HFareDetail> HFareDetails
        {
            get
            {
                if (this.m_HFareDetails == null)
                {
                    this.m_HFareDetails = base.CreateQuery<HFareDetail>();
                }
                return this.m_HFareDetails;
            }
        }

        #endregion
    }
}
