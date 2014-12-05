using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM
{
    public static class GLOBALS
    {
        // The Data Source GLOBALS
        public static class DataSource
        {
            public enum Type
            {
                // Data Source Types
                [Description("Default Value")]
                [DefaultValue("N/A")]
                Default = 0,

                [Description("Data is read from a database table.")]
                [DefaultValue("DBTable")]
                DBTable = 1,

                [Description("Data is read from a webservice endpoint.")]
                [DefaultValue("WebService")]
                WebService = 2
            }

            // Data Source Access Methods
            public enum AccessMethod
            {
                [Description("Default Value")]
                Default = 0,

                [Description("Data is read from a single source, such as: a table, a webservice endpoint...etc")]
                SingleSource = 1,

                [Description("Data is read from multiple sources, this data source acts as a lookup of the other sources, such as: a lookup table, a lookup webservice endpoint...etc")]
                DistributedSource = 2
            }
        }

        // The Data Relation GLOBALS
        public static class DataRelation
        {
            public enum Type
            {
                [Description("The intersection of two data models. Equivalent to an SQL INNER JOIN.")]
                [DefaultValue("INTERSECTION")]
                INTERSECTION = 0,

                [Description("The union of two data models. Equivalent to an SQL OUTER JOIN.")]
                [DefaultValue("UNION")]
                UNION = 1
            }
        }
    }
}
