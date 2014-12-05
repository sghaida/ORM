using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace ORM.DataAccess
{
    public interface IDataAccess<T> where T : class, new()
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataObject"></param>
        /// <returns></returns>
        int Insert(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataObject"></param>
        /// <param name="dataSourceName"></param>
        /// <param name="dataSource"></param>
        /// <returns></returns>
        //bool Update(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSource = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataObject"></param>
        /// <param name="whereConditions"></param>
        /// <param name="dataSourceName"></param>
        /// <param name="dataSourceType"></param>
        /// <returns></returns>
        bool Update(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataObject"></param>
        /// <param name="dataSourceName"></param>
        /// <param name="dataSourceType"></param>
        /// <returns></returns>
        bool Delete(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="dataSourceName"></param>
        /// <param name="dataSourceType"></param>
        /// <param name="IncludeDataRelations"></param>
        /// <returns></returns>
        T GetById(long id, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// Gets the data from repository 
        /// </summary>
        /// <param name="fields">List<string> represents the fields that should be set</param>
        /// <param name="where">Dictionary<string,object> Represents the where part that should be executed</param>
        /// <param name="limit">Number of T objects to be populated</param>
        /// <returns>IQueryable<T>  Results</returns>
        IEnumerable<T> Get(Dictionary<string, object> where, int limit = 25, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// Gets the data from the repository and filter it based on the specified predicate expression
        /// </summary>
        /// <param name="predicate">Expression<Func<T, bool>> predicate specify the expression that should be evaluated</param>
        /// <returns>IQueryable<T>  Results</returns>
        IEnumerable<T> Get(Expression<Func<T, bool>> predicate, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);

        /// <summary>
        /// Get all the data from the Repo
        /// </summary>
        /// <returns></returns>
        IEnumerable<T> GetAll(string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default);


        #region Native SQL Execute Commands

        IEnumerable<T> GetAll(string sql);

        int Insert(string sql);

        bool Update(string sql);

        bool Delete(string sql);

        #endregion



    }
}
