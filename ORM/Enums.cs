using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ORM
{
    public class Enums
    {
        public enum DataSourceType
        {
            Default,
            [Description("Data is read from a database table.")]
            DBTable,
            [Description("Data is read from a webservice endpoint.")]
            WS
        }

        public enum DataSourceAccessType
        {
            Default,
            [Description("Data is read from a single source, such as: a table, a webservice endpoint...etc")]
            SingleSource,
            [Description("Data is read from multiple sources, this data source acts as a lookup of the other sources, such as: a lookup table, a lookup webservice endpoint...etc")]
            Distributed
        }

        public enum DataRelationType
        {
            [Description("The intersection of two data models. Equivalent to an SQL INNER JOIN.")]
            INTERSECTION = 0,
            [Description("The union of two data models. Equivalent to an SQL OUTER JOIN.")]
            UNION = 1
        }

        /// <summary>
        /// Gets the Name of DB table Field
        /// </summary>
        /// <param name="value">Enum Name</param>
        /// <returns>Field Description</returns>
        public static string GetDescription(Enum value)
        {
            FieldInfo fieldInfo = value.GetType().GetField(value.ToString());

            DescriptionAttribute[] descAttributes = (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);

            if (descAttributes != null && descAttributes.Length > 0)
                return descAttributes[0].Description;
            else
                return value.ToString();
        }

        /// <summary>
        /// Gets the DefaultValue attribute of the enum
        /// </summary>
        /// <param name="value">Enum Name</param>
        /// <returns>Field Description</returns>
        public static string GetValue(Enum value)
        {
            FieldInfo fieldInfo = value.GetType().GetField(value.ToString());

            DefaultValueAttribute[] valueAttributes = (DefaultValueAttribute[])fieldInfo.GetCustomAttributes(typeof(DefaultValueAttribute), false);

            if (valueAttributes != null && valueAttributes.Length > 0)
                return valueAttributes[0].Value.ToString();
            else
                return value.ToString();
        }

        public static IEnumerable<T> EnumToList<T>()
        {
            Type enumType = typeof(T);

            if (enumType.BaseType != typeof(Enum))
                throw new ArgumentException("T is not of System.Enum Type");

            Array enumValArray = Enum.GetValues(enumType);
            List<T> enumValList = new List<T>(enumValArray.Length);

            foreach (int val in enumValArray)
            {
                enumValList.Add((T)Enum.Parse(enumType, val.ToString()));
            }

            return enumValList;
        }
    }
}
