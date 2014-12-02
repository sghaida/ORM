using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

using ORM.DataAccess;
using ORM.DataModels;

namespace ORM.DataMappers
{
    public class SitesDepartmentsDataMapper : DataAccess<SiteDepartment>
    {
        /// <summary>
        /// Given a Site's ID, return the list of it's Departments.
        /// </summary>
        /// <param name="SiteID">Site.ID (int)</param>
        /// <returns>List of SiteDepartment objects</returns>
        public List<SiteDepartment> GetDepartmentsForSite(long SiteID)
        {
            Dictionary<string, object> condition = new Dictionary<string,object>();
            condition.Add("SiteID", SiteID);

            try
            {
                return Get(whereConditions: condition, limit: 0).ToList<SiteDepartment>();
            }
            catch(Exception ex)
            {
                throw ex.InnerException;
            }
        }

    }

}
