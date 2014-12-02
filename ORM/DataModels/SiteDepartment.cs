using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ORM.DataAccess;
using ORM.DataAttributes;

namespace ORM.DataModels
{
    [DataSource(Name = "Sites_Departments", SourceType = Enums.DataSourceType.DBTable, AccessType = Enums.DataSourceAccessType.SingleSource)]
    public class SiteDepartment : DataModel
    {
        [IsIDField]
        [DbColumn("ID")]
        public int ID { get; set; }

        [DbColumn("SiteID")]
        public int SiteID { get; set; }

        [DbColumn("DepartmentID")]
        public int DepartmentID { get; set; }

        [DataRelation(Name = "SiteID_Site.ID", WithDataModel = typeof(Site), OnDataModelKey = "ID", ThisKey = "SiteID")]
        public Site Site { get; set; }

        [DataRelation(Name = "DepartmentID_Department.ID", WithDataModel = typeof(Department), OnDataModelKey = "ID", ThisKey = "DepartmentID")]
        public Department Department { get; set; }
    }
}
