using System;
using System.Reflection;

namespace ORM.Helpers
{
    // Helper class for the ConverToList<T> function
    public class ObjectPropertyInfoField
    {
        public string DataFieldName { get; set; }
        public string ObjectFieldName { get; set; }
        public PropertyInfo Property { get; set; }
        public Type DataFieldType { get; set; }
    }
}