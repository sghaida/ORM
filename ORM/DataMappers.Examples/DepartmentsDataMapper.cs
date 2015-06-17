using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

using ORM.DataAccess;
using ORM.DataModels.Examples;

namespace ORM.DataMappers.Examples
{
    public class DepartmentsDataMapper : DataAccess<Department>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="departmentName"></param>
        /// <returns></returns>
        public Department GetByName(string departmentName)
        {
            Department department = null;
            var condition = new Dictionary<string, object>();
            condition.Add("DepartmentName", departmentName);

            try
            {
                var results = base.Get(whereConditions: condition, limit: 1).ToList<Department>();

                if (results != null && results.Count > 0)
                {
                    department = results.First();
                }

                return department;
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

    }
}
