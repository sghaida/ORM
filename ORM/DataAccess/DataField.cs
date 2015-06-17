using System;

namespace ORM.DataAccess
{
    /// <summary>
    ///     This is a wrapper class used to describe the Data Model attributes, properties and relations. Each attribute has a
    ///     Data Field object to describe it.
    ///     This is associated with the DataSourceSchema class.
    /// </summary>
    public class DataField
    {
        /// <summary>
        ///     The class field name that correspondes to this data field.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     The class field database properties. This describes it's possible relation to a database table's column.
        /// </summary>
        public DbTableField TableField { get; set; }

        /// <summary>
        ///     The class field database tables-relation. This describes what database-relation can fill this data-field's content.
        /// </summary>
        public DbRelation Relation { get; set; }
    }


    /// <summary>
    ///     This class is associated with describing the class attributes that are marked with at least on of the following
    ///     Decorators or Data Attributes.
    ///     * DbColumn Attribute
    ///     * IsIDField Attribute
    ///     * AllowNull Attribute
    ///     * AllowIDInsert Attribute
    /// </summary>
    public class DbTableField
    {
        /// <summary>
        ///     The corresponding database table field.
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        ///     A true or false flag to indicate whether this is an ID field in it's table or not.
        /// </summary>
        public bool IsIdField { get; set; }

        /// <summary>
        ///     A true or false flag to indicate whether this value can be null on update or insert
        /// </summary>
        public bool AllowNull { get; set; }

        /// <summary>
        ///     A true or false flag to indicate whether this ID Field can be inserted and/or updated in it's table.
        /// </summary>
        public bool AllowIdInsert { get; set; }

        /// <summary>
        ///     A true or false flag to indicate whether this is a foreign key in it's table or not.
        /// </summary>
        public bool IsKey { get; set; }

        /// <summary>
        ///     This attribute tells the Repository that it's associated property (DbColumn) can be excluded on Select.
        /// </summary>
        public bool ExcludeOnSelect { get; set; }

        /// <summary>
        ///     This attribute tells the Repository that it's associated property (DbColumn) can be excluded on Insert.
        /// </summary>
        public bool ExcludeOnInsert { get; set; }

        /// <summary>
        ///     This attribute tells the Repository that it's associated property (DbColumn) can be excluded on Update.
        /// </summary>
        public bool ExcludeOnUpdate { get; set; }

        /// <summary>
        ///     The reflected field type in the class.
        /// </summary>
        public Type FieldType { get; set; }
    }


    /// <summary>
    ///     This class is associated with describing the class attributes that are marked with the Data Relation Decorator
    ///     (Data Attribute).
    ///     * DataRelation Attribute
    /// </summary>
    public class DbRelation
    {
        /// <summary>
        ///     The object that will hold the data returned from the relation query
        /// </summary>
        public string DataField { get; set; }

        /// <summary>
        ///     The descriptive relation name
        /// </summary>
        public string RelationName { get; set; }

        /// <summary>
        ///     The data model type this relation is associated with
        /// </summary>
        public Type WithDataModel { get; set; }

        /// <summary>
        ///     The data modle key this relation is defined on
        /// </summary>
        public string OnDataModelKey { get; set; }

        /// <summary>
        ///     The class instance field name that shares the relation with the destination data model
        /// </summary>
        public string ThisKey { get; set; }

        /// <summary>
        ///     This sets the relation type between the two data models.
        ///     It can be any value defined in the GLOBALS.DataRelation.Type
        /// </summary>
        public Globals.DataRelation.Type RelationType { get; set; }

        /*
                private static T Cast<T>(object o)
                {
                    return (T) o;
                }
        */
    }
}