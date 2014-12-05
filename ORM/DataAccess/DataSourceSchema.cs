using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using ORM;
using ORM.DataAttributes;


namespace ORM.DataAccess
{
    public class DataSourceSchema<T> where T: DataModel, new()
    {
        public string DataSourceName { get; set; }
        public GLOBALS.DataSource.Type DataSourceType { set; get; }
        public GLOBALS.DataSource.AccessMethod DataSourceAccessMethod { get; set; }

        public string IDFieldName { set; get; }

        public List<DataField> DataFields { get; set; }


        /***
        * Private functions.
        */
        /// <summary>
        /// Tries to read the TableName attribute value if it exists; if it doesn't it throws and exception
        /// </summary>
        /// <returns>TableName attribute value (string), if exists.</returns>
        private void tryReadDataSourceAttributeValue()
        {
            //Get the table name attribute
            IEnumerable<Attribute> dataSourceAtt = typeof(T).GetCustomAttributes(typeof(DataSourceAttribute));

            // This mean that the Class is unstructured Class and it could be related to table/function or procedure or not.
            if (dataSourceAtt.Count() > 0)
            {
                var dsAttr = ((DataSourceAttribute)dataSourceAtt.First());

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
        }

        /// <summary>
        /// Tries to read the Class Db Properties, which are the properties marked with DbColumn Attribute. It tries to resolve the other attribute values, if they exist, 
        /// otherwise, it assigns the default values.
        /// Write the results to the inner List of DataFields
        /// </summary>
        private void tryReadClassDataFields()
        {
            this.DataFields = new List<DataField>();

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
            foreach (var field in allClassFields)
            {
                var newDataField = new DataField();

                newDataField.Name = field.Name;

                if (field.GetCustomAttribute<DbColumnAttribute>() != null)
                {
                    newDataField.TableField = new DbTableField()
                    {
                        ColumnName = field.GetCustomAttribute<DbColumnAttribute>().Name,
                        IsIDField = field.GetCustomAttribute<IsIDFieldAttribute>() != null ? field.GetCustomAttribute<IsIDFieldAttribute>().Status : false,
                        AllowNull = field.GetCustomAttribute<AllowNullAttribute>() != null ? field.GetCustomAttribute<AllowNullAttribute>().Status : false,
                        AllowIDInsert = field.GetCustomAttribute<AllowIDInsertAttribute>() != null ? field.GetCustomAttribute<AllowIDInsertAttribute>().Status : false,
                        IsKey = field.GetCustomAttribute<IsKeyAttribute>() != null ? field.GetCustomAttribute<IsKeyAttribute>().Status : false,
                        FieldType = field.PropertyType
                    };
                }

                if (field.GetCustomAttribute<DataRelationAttribute>() != null)
                {
                    var dataRelationAttribute = field.GetCustomAttribute<DataRelationAttribute>();

                    newDataField.Relation = new DbRelation()
                    {
                        DataField = field.Name,
                        RelationName = dataRelationAttribute.Name,
                        WithDataModel = dataRelationAttribute.WithDataModel,
                        OnDataModelKey = dataRelationAttribute.OnDataModelKey,
                        ThisKey = dataRelationAttribute.ThisKey,
                        RelationType = dataRelationAttribute.RelationType
                    };
                }

                this.DataFields.Add(newDataField);
            }

            //Set the IDFieldName variable to the DbColumn name of the ID.
            if (this.DataFields.Count > 0)
            {
                var field = this.DataFields.Find(item => item.TableField != null && item.TableField.IsIDField == true);

                if (field != null)
                {
                    this.IDFieldName = field.TableField.ColumnName;
                }
            }
        }


        /// <summary>
        /// Constructor.
        /// Calls the private setters to initialize the schema over the DataModel T
        /// </summary>
        public DataSourceSchema()
        {
            try
            {
                tryReadDataSourceAttributeValue();
                tryReadClassDataFields();
            }
            catch (Exception ex)
            {
                throw ex.InnerException;
            }
        }


        /***
         * Getters.
         * They support accessing a dynamic version of this object's data
         */
        public string GetDataSourceName()
        {
            return this.DataSourceName;
        }


        public GLOBALS.DataSource.Type GetDataSourceType()
        {
            return this.DataSourceType;
        }


        public GLOBALS.DataSource.AccessMethod GetDataSourceAccessMethod()
        {
            return this.DataSourceAccessMethod;
        }


        public string GetIDFieldName()
        {
            return this.IDFieldName;
        }


        public List<DataField> GetDataFields()
        {
            return this.DataFields;
        }


    }

}
