using ORM.Libs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ORM.DataAccess;
using ORM.DataAttributes;

namespace ORM.DataModels.Examples
{
    [DataSource(Name = "Departments", Type = Globals.DataSource.Type.DbTable, AccessMethod = Globals.DataSource.AccessMethod.SingleSource)]
    public class Department : DataModel
    {
        [IsIdField]
        [DbColumn("ID")]
        public int ID { get; set; }

        [DbColumn("DepartmentName")]
        public string Name { get; set; }

        [AllowNull]
        [DbColumn("Description")]
        public string Description { get; set; }
    }
}
