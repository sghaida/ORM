using System;
using System.Collections.Generic;

namespace ORM.Helpers
{
    public class ReflectionHelper<T> where T : class, new()
    {
        /// <summary>
        ///     Returns an empty dictionary of strings and actions
        ///     This is needed in the DataAccess and DataTable helpers to reflect on types within types
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, Action<T, object>> GetReflectionDictionary()
        {
            return (new Dictionary<string, Action<T, object>>());
        }
    }
}