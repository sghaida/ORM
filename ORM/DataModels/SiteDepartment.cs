using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ORM.DataAccess;
using ORM.DataAttributes;

namespace ORM.DataModels
{
    [DataSource(Name = "Sites_Departments", Type = GLOBALS.DataSource.Type.DBTable, AccessMethod = GLOBALS.DataSource.AccessMethod.SingleSource)]
    public class SiteDepartment : DataModel
    {
        [IsIDField]
        [DbColumn("ID")]
        public int ID { get; set; }

        [DbColumn("SiteID")]
        public int SiteID { get; set; }

        [DbColumn("DepartmentID")]
        public int DepartmentID { get; set; }

        [DataRelation(WithDataModel = typeof(Site), OnDataModelKey = "ID", ThisKey = "SiteID")]
        public Site Site { get; set; }

        [DataRelation(WithDataModel = typeof(Department), OnDataModelKey = "ID", ThisKey = "DepartmentID")]
        public Department Department { get; set; }
    }
}
