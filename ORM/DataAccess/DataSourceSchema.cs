using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ORM.DataAttributes;

namespace ORM.DataAccess
{
    public class DataSourceSchema<T> where T : DataModel, new()
    {
        /// <summary>
        ///     Constructor.
        ///     Calls the private setters to initialize the schema over the DataModel T
        /// </summary>
        public DataSourceSchema()
        {
            try
            {
                TryReadDataSourceAttributeValue();
                TryReadClassDataFields();
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }

        public string DataSourceName { get; set; }
        public Globals.DataSource.Type DataSourceType { set; get; }
        public Globals.DataSource.AccessMethod DataSourceAccessMethod { get; set; }
        public string IdFieldName { set; get; }
        public List<DataField> DataFields { get; set; }
        /***
        * Private functions.
        */

        /// <summary>
        ///     Tries to read the TableName attribute value if it exists; if it doesn't it throws and exception
        /// </summary>
        /// <returns>TableName attribute value (string), if exists.</returns>
        private void TryReadDataSourceAttributeValue()
        {
            //Get the table name attribute
            var dataSourceAtt = typeof(T).GetCustomAttributes(typeof(DataSourceAttribute));

            // This mean that the Class is unstructured Class and it could be related to table/function or procedure or not.
            var sourceAtt = dataSourceAtt as IList<Attribute> ?? dataSourceAtt.ToList();
            if (!sourceAtt.Any()) return;

            var dsAttr = ((DataSourceAttribute)sourceAtt.First());

            if (dsAttr != null)
            {
                DataSourceType = dsAttr.Type;
                DataSourceAccessMethod = dsAttr.AccessMethod;

                if (false == string.IsNullOrEmpty(dsAttr.Name))
                {
                    DataSourceName = dsAttr.Name;
                }
            }
        }

        /// <summary>
        ///     Tries to read the Class Db Properties, which are the properties marked with DbColumn Attribute. It tries to resolve
        ///     the other attribute values, if they exist,
        ///     otherwise, it assigns the default values.
        ///     Write the results to the inner List of DataFields
        /// </summary>
        private void TryReadClassDataFields()
        {
            DataFields = new List<DataField>();

            var tableFields = typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .ToList();

            var relationFields = typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(property => property.GetCustomAttribute<DataRelationAttribute>() != null)
                .ToList();

            var allClassFields = tableFields.Concat(relationFields).ToList();

            //If no exception was thrown, proceed to processing the class fields
            foreach (var propertyInfo in allClassFields)
            {
                var newDataField = new DataField();

                newDataField.Name = propertyInfo.Name;

                if (propertyInfo.GetCustomAttribute<DbColumnAttribute>() != null)
                {
                    var dbColumnAttr = propertyInfo.GetCustomAttribute<DbColumnAttribute>();
                    var isIdFieldAttr = propertyInfo.GetCustomAttribute<IsIdFieldAttribute>();
                    var allowNullAttr = propertyInfo.GetCustomAttribute<AllowNullAttribute>();
                    var allowIdInsertAttr = propertyInfo.GetCustomAttribute<AllowIdInsertAttribute>();
                    var isKeyAttr = propertyInfo.GetCustomAttribute<IsForeignKeyAttribute>();
                    var excludeAttr = propertyInfo.GetCustomAttribute<ExcludeAttribute>();

                    newDataField.TableField = new DbTableField
                    {
                        ColumnName = dbColumnAttr.Name,
                        IsIdField = isIdFieldAttr != null && isIdFieldAttr.Status,
                        AllowNull = allowNullAttr != null && allowNullAttr.Status,
                        AllowIdInsert = allowIdInsertAttr != null && allowIdInsertAttr.Status,
                        IsKey = isKeyAttr != null && isKeyAttr.Status,
                        ExcludeOnSelect = excludeAttr != null && excludeAttr.OnSelect,
                        ExcludeOnInsert = excludeAttr != null && excludeAttr.OnInsert,
                        ExcludeOnUpdate = excludeAttr != null && excludeAttr.OnUpdate,
                        FieldType = propertyInfo.PropertyType
                    };
                }

                if (propertyInfo.GetCustomAttribute<DataRelationAttribute>() != null)
                {
                    var dataRelationAttribute = propertyInfo.GetCustomAttribute<DataRelationAttribute>();

                    newDataField.Relation = new DbRelation
                    {
                        DataField = propertyInfo.Name,
                        RelationName = dataRelationAttribute.Name,
                        WithDataModel = dataRelationAttribute.WithDataModel,
                        OnDataModelKey = dataRelationAttribute.OnDataModelKey,
                        ThisKey = dataRelationAttribute.ThisKey,
                        RelationType = dataRelationAttribute.RelationType
                    };
                }

                DataFields.Add(newDataField);
            }

            //Set the IDFieldName variable to the DbColumn name of the ID.
            if (DataFields.Count <= 0) return;

            var field = DataFields.Find(item => item.TableField != null && item.TableField.IsIdField);

            if (field != null)
            {
                IdFieldName = field.TableField.ColumnName;
            }
        }

        /***
         * Getters.
         * They support accessing a dynamic version of this object's data
         */

        public string GetDataSourceName()
        {
            return DataSourceName;
        }

        public Globals.DataSource.Type GetDataSourceType()
        {
            return DataSourceType;
        }

        public Globals.DataSource.AccessMethod GetDataSourceAccessMethod()
        {
            return DataSourceAccessMethod;
        }

        public string GetIdFieldName()
        {
            return IdFieldName;
        }

        public List<DataField> GetDataFields()
        {
            return DataFields;
        }
    }
}