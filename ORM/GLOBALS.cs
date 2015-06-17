using System.ComponentModel;

namespace ORM
{
    public static class Globals
    {
        //
        // The Data Source GLOBALS
        public static class DataSource
        {
            // Data Source Access Methods
            public enum AccessMethod
            {
                [Description("Default Value")] Default = 0,

                [Description("Data is read from a single source, such as: a table, a webservice endpoint...etc")] SingleSource = 1,

                [Description(
                    "Data is read from multiple sources, this data source acts as a lookup of the other sources, such as: a lookup table, a lookup webservice endpoint...etc"
                    )] DistributedSource = 2
            }

            public enum Type
            {
                // Data Source Types
                [Description("Default Value")] [DefaultValue("N/A")] Default = 0,

                [Description("Data is read from a database table.")] [DefaultValue("DBTable")] DbTable = 1,

                [Description("Data is read from a webservice endpoint.")] [DefaultValue("WebService")] WebService = 2
            }
        }


        //
        // The Data Relation GLOBALS
        public static class DataRelation
        {
            public enum Type
            {
                [Description("The intersection of two data models. Equivalent to an SQL INNER JOIN.")] [DefaultValue("INTERSECTION")] Intersection = 0,

                [Description("The union of two data models. Equivalent to an SQL OUTER JOIN.")] [DefaultValue("UNION")] Union = 1
            }
        }

    }

}