using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;
using System.Reflection;
using ORM.DataAttributes;

namespace ORM.Helpers
{
    public static class DataReaderExtension
    {
        public static T ConvertToObject<T>(this OleDbDataReader dataReader) where T : class, new()
        {
            var dataObj = new T();

            //var cdtPropertyInfo = new Dictionary<string, List<ObjectPropertyInfoField>>();


            //List of T object data fields (DbColumnAttribute Values), and types.
            //var masterObjectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            // Initialize Master the property info fields list
            typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .ToList();

            return dataObj;
        }
    }
}