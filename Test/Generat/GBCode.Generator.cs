﻿#region AgileEAS.NET-generated
//------------------------------------------------------------------------------
//     AgileEAS.NET应用开发平台，是基于敏捷并行开发思想以及.NET构件技术而开发的一个应用系统快速开发平台，用于帮助中小软件企业
//建立一条适合快速变化的开发团队，以达到节省开发成本、缩短开发时间，快速适应市场变化的目的。
//     AgileEAS.NET应用开发平台包含基础类库、资源管理平台、运行容器、开发辅助工具等四大部分，资源管理平台为敏捷并行开发提供了
//设计、实现、测试等开发过程的并行，应用系统的各个业务功能子系统，在系统体系结构设计的过程中被设计成各个原子功能模块，各个子
//功能模块按照业务功能组织成单独的程序集文件，各子系统开发完成后，由AgileEAS.NET资源管理平台进行统一的集成部署。
//
//     AgileEAS.NET SOA 中间件平台是一套免费的快速开发工具，可以不受限制的用于各种非商业开发之中，商业应用请向作者获取商业授权，
//商业授权也是免费的，但是对于非授权的商业应用视为侵权，开发人员可以参考官方网站和博客园等专业网站获取公开的技术资料，也可以向
//AgileEAS.NET官方团队请求技术支持。
//
// 官方网站：http://www.smarteas.net
// 团队网站：http://www.agilelab.cn
//------------------------------------------------------------------------------
// <auto-generated>
//     此代码由AgileEAS.NET数据模型设计工具生成。
//     运行时版本:4.0.30319.1
//
//     对此文件的更改可能会导致不正确的行为，并且如果
//     重新生成代码，这些更改将会丢失。
// </auto-generated>
//------------------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using System.ComponentModel;
using System.Data;
using EAS.Data;
using EAS.Data.Access;
using EAS.Data.ORM;
using EAS.Data.Linq;

namespace AgileHIS.Entities
{
   /// <summary>
   /// 实体对象 GBCode(国标码)。
   /// </summary>
   [Serializable()]
   [DataContract(IsReference=true)]
   [Table("IM_GBCODE","国标码")]
   partial class GBCode: DataEntity<GBCode>, IDataEntity<GBCode>
   {
       public GBCode()
       {
       }
       
       protected GBCode(SerializationInfo info, StreamingContext context)
           : base(info, context)
       {
       }
       
       #region O/R映射成员

       /// <summary>
       /// 分类 。
       /// </summary>
       [Column("CATEGORY","分类")]
       [DisplayName("分类")]
       [DataMember]
       public int Category
       {
           get;
           set;
       }

       /// <summary>
       /// 编码 。
       /// </summary>
       [Column("CODE","编码"),PrimaryKey,NotNull]
       [DisplayName("编码")]
       [DataMember]
       public int Code
       {
           get;
           set;
       }

       /// <summary>
       /// 名称 。
       /// </summary>
       [Column("NAME","名称"),DataSize(128)]
       [DisplayName("名称")]
       [DataMember]
       public string Name
       {
           get;
           set;
       }

       /// <summary>
       /// 属性 0-禁用 1-启用。
       /// </summary>
       [Column("ATTRIBUTE","属性")]
       [DisplayName("属性")]
       [DataMember]
       public int Attribute
       {
           get;
           set;
       }

       /// <summary>
       /// 排序码 。
       /// </summary>
       [Column("SORTCODE","排序码")]
       [DisplayName("排序码")]
       [DataMember]
       public int SortCode
       {
           get;
           set;
       }

       /// <summary>
       /// 标志码 。
       /// </summary>
       [Column("SYMBOL","标志码"),DataSize(128)]
       [DisplayName("标志码")]
       [DataMember]
       public string Symbol
       {
           get;
           set;
       }

       /// <summary>
       /// 输入码1 。
       /// </summary>
       [Column("INPUTCODE1","输入码1"),DataSize(128)]
       [DisplayName("输入码1")]
       [DataMember]
       public string InputCode1
       {
           get;
           set;
       }

       /// <summary>
       /// 输入码2 。
       /// </summary>
       [Column("INPUTCODE2","输入码2"),DataSize(128)]
       [DisplayName("输入码2")]
       [DataMember]
       public string InputCode2
       {
           get;
           set;
       }

       /// <summary>
       /// 英文名称 。
       /// </summary>
       [Column("ALIAS","英文名称"),DataSize(128)]
       [DisplayName("英文名称")]
       [DataMember]
       public string Alias
       {
           get;
           set;
       }

       /// <summary>
       /// 国家编码 。
       /// </summary>
       [Column("STANDARDCODE","国家编码"),DataSize(128)]
       [DisplayName("国家编码")]
       [DataMember]
       public string StandardCode
       {
           get;
           set;
       }

       /// <summary>
       /// 描述 。
       /// </summary>
       [Column("DESCRIPTION","描述"),DataSize(128)]
       [DisplayName("描述")]
       [DataMember]
       public string Description
       {
           get;
           set;
       }

       /// <summary>
       /// 简介 。
       /// </summary>
       [Column("DETAILS","简介"),DataSize(128)]
       [DisplayName("简介")]
       [DataMember]
       public string Details
       {
           get;
           set;
       }

       /// <summary>
       /// 最后更新时间 。
       /// </summary>
       [Column("LMTIME","最后更新时间"),CacheUpdated]
       [DisplayName("最后更新时间")]
       [DataMember]
       public DateTime LMTime
       {
           get;
           set;
       }

       /// <summary>
       /// 最后更新人 。
       /// </summary>
       [Column("LMODIFIER","最后更新人")]
       [DisplayName("最后更新人")]
       [DataMember]
       public int LModifier
       {
           get;
           set;
       }
       
       #endregion
       
       #region O/R虚拟属性
       
       #endregion
       
       #region O/R引用实体
       
       #endregion
       
       #region O/R子实体
       
       #endregion

       public override string ToString()
       {
           return this.Name.ToString();
       }

   }
}
