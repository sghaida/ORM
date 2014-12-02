using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using ORM.Helpers;
//using ORM.DataModels;
using ORM.DataAttributes;
using ORM.Exceptions;


namespace ORM.DataAccess
{
    public class DataAccess<T> : IDataAccess<T> where T : DataModel, new()
    {
        /**
         * Private instance variables
         */
        private DataSourceSchema<T> Schema;

        private static DBLib DBRoutines = new DBLib();

        /// <summary>
        /// This is a private function. It is responsible for returning a list of the data relations on this data model translated to a list of SqlJoinRelation objects.
        /// </summary>
        /// <returns>List of SqlJoinRelation objects</returns>
        private List<SqlJoinRelation> GetDataRelations()
        {
            //Table Relations Map
            //To be sent to the DB Lib for SQL Query generation
            List<SqlJoinRelation> TableRelationsMap = new List<SqlJoinRelation>();

            //TableRelationsList
            //To be used to looking up the relations and extracting information from them and copying them into the TableRelationsMap
            List<DbRelation> DbRelationsList = Schema.DataFields.Where(field => field.Relation != null).Select<DataField, DbRelation>(field => field.Relation).ToList<DbRelation>();

            //Start processing the list of table relations
            if (DbRelationsList != null && DbRelationsList.Count() > 0)
            {
                //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                foreach (var relation in DbRelationsList)
                {
                    //Create a temporary map for this target table relation
                    var joinedTableInfo = new SqlJoinRelation();

                    //Get the data model we're in relation with.
                    Type relationType = relation.WithDataModel;

                    //Build a data source schema for the data model we're in relation with.
                    var generalModelSchemaType = typeof(DataSourceSchema<>);
                    var specialModelSchemaType = generalModelSchemaType.MakeGenericType(relationType);
                    dynamic joinedModelSchema = Activator.CreateInstance(specialModelSchemaType);

                    //Get it's Data Fields.
                    List<DataField> joinedModelFields = joinedModelSchema.GetDataFields();

                    //Get the table column names - exclude the ID field name.
                    List<string> joinedModelTableColumns = joinedModelFields
                        .Where(field => field.TableField != null)
                        .Select<DataField, string>(field => field.TableField.ColumnName)
                        .ToList<string>();

                    //Get the field that describes the relation key from the target model schema
                    DataField joinedModelKey = joinedModelFields.Find(item => item.TableField != null && item.Name == relation.OnDataModelKey);

                    //Get the field that describes our key on which we are in relation with the target model
                    DataField thisKey = Schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                    if (thisKey != null && joinedModelKey != null)
                    {
                        //Initialize the temporary map and add it to the original relations map

                        joinedTableInfo.RelationType = relation.RelationType.ToString();
                        joinedTableInfo.MasterTableName = Schema.DataSourceName;
                        joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                        joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                        //Add the relation keys to the TableRelationsMap
                        TableRelationsMap.Add(joinedTableInfo);
                    }

                }//end-foreach

            }//end-outer-if

            return TableRelationsMap;
        }


        /**
         * Repository Constructor
         */
        public DataAccess() 
        {
            //Get the Table Name and List of Class Attributes
            try
            {
                //Initialize the schema for the class T
                this.Schema = new DataSourceSchema<T>();
                
                //Check for absent or invalid DataModel attributes and throw the respective exception if they exist.
                if(string.IsNullOrEmpty(Schema.DataSourceName))
                {
                    throw new NoDataSourceNameException(typeof(T).Name);
                }
                else if(Schema.DataFields.Where(item => item.TableField != null).ToList().Count() == 0)
                {
                    throw new NoTableFieldsException(typeof(T).Name);
                }
                else if(string.IsNullOrEmpty(Schema.IDFieldName))
                {
                    throw new NoTableIDFieldException(typeof(T).Name);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public virtual int Insert(T dataObject, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default)
        {
            int rowID = 0;
            Dictionary<string, object> columnsValues = new Dictionary<string, object>();
          
            if (dataObject != null)
            {
                var properties = Schema.DataFields.Select(field => field.TableField).ToList();

                foreach (var property in properties)
                {
                    var dataObjectAttr = dataObject.GetType().GetProperty(property.ColumnName);

                    //Don't insert ID Fields into the Database
                    if(property.IsIDField == true)
                    {
                        continue;
                    }

                    //Continue handling the properties
                    if (property.AllowNull == false && dataObjectAttr != null)
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            columnsValues.Add(property.ColumnName, Convert.ChangeType(dataObjectAttrValue, property.FieldType));
                        }
                        else
                        {
                            throw new Exception("The Property " + property.ColumnName + " in the " + dataObject.GetType().Name + " Table is not allowed to be null kindly annotate the property with [IsAllowNull]");
                        }
                    }
                    else
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            columnsValues.Add(property.ColumnName, Convert.ChangeType(dataObjectAttrValue, property.FieldType));
                        }
                    }
                    //end-inner-if

                }//end-foreach

                try
                {
                    rowID = DBRoutines.INSERT(tableName: Schema.DataSourceName, columnsValues: columnsValues, idFieldName: Schema.IDFieldName);
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }//end-outer-if

            return rowID;  
        }


        public virtual bool Delete(T dataObject, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default)
        {
            long ID = 0;

            var dataObjectAttr = dataObject.GetType().GetProperty(Schema.IDFieldName);

            if (dataObjectAttr == null)
            {
                throw new Exception("There is no available ID field. kindly annotate " + typeof(T).Name);
            }
            else 
            {
                var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                if(dataObjectAttrValue == null)
                {
                    throw new Exception("There is no available ID field is presented but not set kindly set the value of the ID field Object for the following class: " + typeof(T).Name);
                }
                else
                {
                    long.TryParse(dataObjectAttrValue.ToString(),out ID);

                    return DBRoutines.DELETE(tableName: Schema.DataSourceName, idFieldName: Schema.IDFieldName, ID: ID);
                }//end-inner-if-else
            }//end-outer-if-else
        }


        public virtual bool Update(T dataObject, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default)
        {
            Dictionary<string, object> columnsValues = new Dictionary<string, object>();
            bool status = false;

            if (dataObject != null)
            {
                var properties = Schema.DataFields.Select(field => field.TableField).ToList();

                foreach (var property in properties)
                {
                    var dataObjectAttr = dataObject.GetType().GetProperty(property.ColumnName);

                    //Don't insert ID Fields into the Database
                    if(property.IsIDField == true)
                    {
                        continue;
                    }

                    //Continue handling the properties
                    if (property.AllowNull == false && dataObjectAttr != null)
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            columnsValues.Add(property.ColumnName, Convert.ChangeType(dataObjectAttrValue, property.FieldType));
                        }
                        else
                        {
                            throw new Exception("The Property " + property.ColumnName + " in the " + dataObject.GetType().Name + " Table is not allowed to be null kindly annotate the property with [IsAllowNull]");
                        }
                    }
                    else
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            columnsValues.Add(property.ColumnName, Convert.ChangeType(dataObjectAttrValue, property.FieldType));
                        }
                    }
                    //end-inner-if

                }//end-foreach

                try
                {
                    status = DBRoutines.UPDATE(tableName: Schema.DataSourceName, columnsValues: columnsValues, wherePart: null);

                }
                catch (Exception ex)
                {
                    throw ex.InnerException;
                }

            }//end-outer-if

            return status;  
        }


        public virtual T GetById(long id, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default, bool IncludeDataRelations = true)
        {
            DataTable dt = new DataTable();
            string finalDataSourceName = string.Empty;

            int maximumLimit = 1;
            List<string> thisModelTableColumns;
            List<SqlJoinRelation> dataRelations;
            Dictionary<string, object> condition;

            string errorMessage = string.Empty;

            //Get our table columns from the schema
            thisModelTableColumns = Schema.DataFields
                .Where(field => field.TableField != null)
                .Select<DataField, string>(field => field.TableField.ColumnName)
                .ToList<string>();

            //Decide on the Data Source Name
            finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? Schema.DataSourceName : dataSourceName);

            //Validate the presence of the ID
            if (id <= 0)
            {
                errorMessage = String.Format("The ID Field is either null or zero. Kindly pass a valid ID. Class name: \"{0}\".", typeof(T).Name);
                throw new Exception(errorMessage);
            }

            //Construct the record ID condition
            condition = new Dictionary<string, object>();
            condition.Add(Schema.IDFieldName, id);

            //Proceed with getting the data
            if (Schema.DataSourceType == Enums.DataSourceType.DBTable)
            {
                switch (IncludeDataRelations)
                {
                    case true:
                        //Get our data relations list (SqlJoinRelation objects)
                        dataRelations = GetDataRelations();
                        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, condition, dataRelations, maximumLimit);
                        break;

                    case false:
                        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, condition, maximumLimit);
                        break;
                }
            }

            //It will either return a data table with one row or zero rows
            if (dt.Rows.Count == 0)
            {
                return (T)Activator.CreateInstance(typeof(T));
            }
            else
            {
                return dt.ConvertToList<T>(IncludeDataRelations).FirstOrDefault<T>() ?? null;
            }
        }


        public virtual IEnumerable<T> Get(Expression<Func<T, bool>> predicate, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default, bool IncludeDataRelations = true)
        {
            DataTable dt = new DataTable();

            if (predicate == null) 
            {
                var errorMessage = string.Format("There is no defined Predicate. {0} ",typeof(T).Name);
                
                throw new Exception(errorMessage);
            }
            else 
            {
                CustomExpressionVisitor ev = new CustomExpressionVisitor();
                
                string whereClause = ev.Translate(predicate);

                if (string.IsNullOrEmpty(dataSourceName))
                {

                    if (string.IsNullOrEmpty(whereClause))
                    {
                        dt = DBRoutines.SELECT(Schema.DataSourceName);
                    }
                    else
                    {
                        dt = DBRoutines.SELECT(Schema.DataSourceName, whereClause);
                    }
                }
                else 
                {
                    if (string.IsNullOrEmpty(whereClause))
                    {
                        dt = DBRoutines.SELECT(dataSourceName);
                    }
                    else
                    {
                        dt = DBRoutines.SELECT(dataSourceName, whereClause);
                    }
                }
            }

            return dt.ConvertToList<T>(IncludeDataRelations);
        }


        public virtual IEnumerable<T> Get(Dictionary<string, object> whereConditions, int limit = 25, string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default, bool IncludeDataRelations = true)
        {
            DataTable dt = new DataTable();
            string finalDataSourceName = string.Empty;
            
            List<string> thisModelTableColumns;
            List<SqlJoinRelation> dataRelations;

            string errorMessage = string.Empty;

            //Get our table columns from the schema
            thisModelTableColumns = Schema.DataFields
                .Where(field => field.TableField != null)
                .Select<DataField, string>(field => field.TableField.ColumnName)
                .ToList<string>();

            //Decide on the Data Source Name
            finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? Schema.DataSourceName : dataSourceName);
            
            //Validate the presence of the where conditions
            if (whereConditions == null || whereConditions.Count  == 0)
            {
                errorMessage = String.Format("The \"whereConditions\" parameter is either null or empty. Kindly pass a valid \"whereConditions\" parameter. Class name: \"{0}\".", typeof(T).Name);
                throw new Exception(errorMessage);
            }


            //Proceed with getting the data
            if (Schema.DataSourceType == Enums.DataSourceType.DBTable)
            {
                switch (IncludeDataRelations)
                {
                    case true:
                        //Get our data relations list (SqlJoinRelation objects)
                        dataRelations = GetDataRelations();
                        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, whereConditions, dataRelations, 0);
                        break;

                    case false:
                        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, limit);
                        break;
                }
            }

            return dt.ConvertToList<T>(IncludeDataRelations);
        }


        public virtual IEnumerable<T> GetAll(string dataSourceName = null, Enums.DataSourceType dataSource = Enums.DataSourceType.Default, bool IncludeDataRelations = true)
        {
            DataTable dt = new DataTable();
            string finalDataSourceName = string.Empty;

            int maximumLimit = 0;
            List<string> thisModelTableColumns;
            Dictionary<string, object> whereConditions = null;
            List<SqlJoinRelation> dataRelations;

            //Get our table columns from the schema
            thisModelTableColumns = Schema.DataFields
                .Where(field => field.TableField != null)
                .Select<DataField, string>(field => field.TableField.ColumnName)
                .ToList<string>();

            //Decide on the Data Source Name
            finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? Schema.DataSourceName : dataSourceName);
            
            //Proceed with getting the data
            if (Schema.DataSourceType == Enums.DataSourceType.DBTable)
            {
                switch(IncludeDataRelations)
                { 
                    case true:
                        //Get our data relations list (SqlJoinRelation objects)
                        dataRelations = GetDataRelations();
                        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, null, dataRelations, 0);
                        break;

                    case false:
                        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, maximumLimit);
                        break;
                }
            }

            return dt.ConvertToList<T>(IncludeDataRelations);
        }


        public virtual IEnumerable<T> GetAll(string sql)
        {
            DataTable dt = DBRoutines.SELECTFROMSQL(sql);

            return dt.ConvertToList<T>();
        }


        public virtual int Insert(string sql)
        {
            int id = DBRoutines.INSERT(sql);
            
            return id;
        }


        public virtual bool Update(string sql)
        {
            bool status = DBRoutines.UPDATE(sql);

            return status;
        }


        public virtual bool Delete(string sql)
        {
            bool status = DBRoutines.DELETE(sql);

            return status;
        }

    }

}
