using ORM.DataAccess;
using ORM.DataAttributes;
using ORM.Excepsions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using ORM.Helpers;

namespace ORM.DataAccess
{
    public class DataAccess<T> : IDataAccess<T> where T : DataModel, new()
    {
        private DataSourceSchema<T> Schema;

        private static DBLib DBRoutines = new DBLib();
        private static readonly List<Type> NumericTypes = new List<Type>() { typeof(int), typeof(long), typeof(Int16), typeof(Int32), typeof(Int64) };


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
                        joinedTableInfo.RelationName = relation.RelationName;
                        joinedTableInfo.RelationType = relation.RelationType;
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


        public virtual int Insert(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
        {
            int rowID = 0;
            string finalDataSourceName = string.Empty;
            Dictionary<string, object> columnsValues = new Dictionary<string, object>();
            

            //
            // Decide the DataSource Name
            if(false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if(false == string.IsNullOrEmpty(Schema.DataSourceName))
            {
                finalDataSourceName = Schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name + ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Process the data object and attempt to insert it into the data source
            if (dataObject != null)
            {
                // Get only the Data Fields from Schema which have TableFields objects
                var objectSchemaFields = Schema.DataFields
                    .Where(field => field.TableField != null)
                    .ToList<DataField>();

                foreach (var field in objectSchemaFields)
                {
                    // Don't insert the ID Field in the Data Source, unless it's marked as AllowIDInsert
                    if (field.TableField.IsIDField == true && field.TableField.AllowIDInsert == false)
                    {
                        continue;
                    }

                    // Get the property value
                    var dataObjectAttr = dataObject.GetType().GetProperty(field.Name);

                    //Continue handling the properties
                    if (field.TableField.AllowNull == false && dataObjectAttr != null)
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            //
                            // Only update the int/long values to zeros if they are not foreign keys
                            if (true == NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey == true)
                                {
                                    //continue;
                                    throw new Exception("The Property " + field.TableField.ColumnName + " in the " + dataObject.GetType().Name + " Table is a foreign key and it is not allowed to be null. Kindly set the property value.");
                                }
                            }
                            
                            columnsValues.Add(field.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                        else
                        {
                            throw new Exception("The Property " + field.TableField.ColumnName + " in the " + dataObject.GetType().Name + " Table is not allowed to be null kindly annotate the property with [IsAllowNull]");
                        }
                    }
                    else
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            //
                            // Only update the int/long values to zeros if they are not foreign keys
                            if (true == NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey == true)
                                {
                                    continue;
                                }
                            }
                            
                            columnsValues.Add(field.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                    }
                    //end-inner-if

                }//end-foreach

                try
                {
                    rowID = DBRoutines.INSERT(tableName: finalDataSourceName, columnsValues: columnsValues, idFieldName: Schema.IDFieldName);
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }//end-outer-if

            return rowID;  
        }

        
        public virtual bool Update(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
        {
            bool status = false;
            string finalDataSourceName = string.Empty;
            Dictionary<string, object> columnsValues = new Dictionary<string, object>();
            Dictionary<string, object> whereConditions = new Dictionary<string, object>();

            //
            // Decide the DataSource Name 
            if (false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if (false == string.IsNullOrEmpty(Schema.DataSourceName))
            {
                finalDataSourceName = Schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name + ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Process the data object and attempt to insert it into the data source
            if (dataObject != null)
            {
                // Get only the Data Fields from Schema which have TableFields objects
                var objectSchemaFields = Schema.DataFields
                    .Where(field => field.TableField != null)
                    .ToList<DataField>();

                foreach (var field in objectSchemaFields)
                {
                    // Get the property value
                    var dataObjectAttr = dataObject.GetType().GetProperty(field.Name);

                    //
                    // Don't update the ID Field in the Data Source, unless it's marked as AllowIDInsert
                    // Add the data object ID into the WHERE CONDITIONS
                    if (field.TableField.IsIDField == true)
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        //
                        // Put the ID Field in the WHERE CONDITIONS
                        if (dataObjectAttrValue != null)
                        {
                            //
                            // Add the ID Field and Value to the Where Conditions if it was not added already!
                            if (false == whereConditions.Keys.Contains(field.TableField.ColumnName))
                            { 
                                whereConditions.Add(field.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                            }
                        }
                        else
                        {
                            throw new Exception("The Property " + field.TableField.ColumnName + " in the " + dataObject.GetType().Name + " Table is not SET! Kindly please set it to it's original value in order to decide what data to update accordingly.");
                        }


                        //
                        // DON'T CONTINUE EXECUTION IF THE ID FIELD IS NOT ALLOWED TO BE CHANGED
                        if(false == field.TableField.AllowIDInsert)
                        { 
                            continue;
                        }
                    }

                    // 
                    // Add the data object fields into the COLUMNS-VALUES dictionary
                    // This dictionary contains the values to be updated
                    if (field.TableField.AllowNull == false && dataObjectAttr != null)
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            //
                            // Only update the int/long values to zeros if they are not foreign keys
                            if (true == NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey == true)
                                {
                                    //continue;
                                    throw new Exception("The Property " + field.TableField.ColumnName + " in the " + dataObject.GetType().Name + " Table is a foreign key and it is not allowed to be null. Kindly set the property value.");
                                }
                            }

                            columnsValues.Add(field.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                    }
                    else
                    {
                        var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                        if (dataObjectAttrValue != null)
                        {
                            //
                            // Only update the int/long values to zeros if they are not foreign keys
                            if (true == NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey == true)
                                {
                                    continue;
                                }
                            }

                            columnsValues.Add(field.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                    }
                    //end-inner-if

                }//end-foreach

                try
                {
                    if (0 == whereConditions.Count)
                    {
                        throw new Exception("Update Error: Cannot update data object unless there is at least one WHERE CONDITION. Please revise the update procedures on " + dataObject.GetType().Name);
                    }
                    else
                    {
                        status = DBRoutines.UPDATE(tableName: finalDataSourceName, columnsValues: columnsValues, wherePart: whereConditions);
                    }
                }
                catch (Exception ex)
                {
                    throw ex.InnerException;
                }

            }//end-outer-if

            return status;
        }


        public virtual bool Delete(T dataObject, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
        {
            long ID = 0;
            string finalDataSourceName = string.Empty;
            Dictionary<string, object> whereConditions = new Dictionary<string, object>();

            DataField IDField;
            string ObjectFieldNameWithIDAttribute = string.Empty;


            //
            // Decide the DataSource Name 
            if (false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if (false == string.IsNullOrEmpty(Schema.DataSourceName))
            {
                finalDataSourceName = Schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name + ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Decide the IDField value
            IDField = Schema.DataFields.Find(field => field.TableField != null && field.TableField.IsIDField == true);
            
            if(null == IDField)
            {
                throw new Exception("Delete Error: The Data Model does not have IDField property. Kindly mark the properties of " + typeof(T).Name + " with [IsIDField].");
            }


            //
            // Get the object field that is marked with the IsIDField attribute
            var dataObjectAttr = dataObject.GetType().GetProperty(IDField.Name);

            var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

            if(dataObjectAttrValue == null)
            {
                throw new Exception("The ID Field's value is to NULL. Kindly set the value of the ID Field for the object of type: " + typeof(T).Name);
            }
            else
            {
                //long.TryParse(dataObjectAttrValue.ToString(), out ID);
                //return DBRoutines.DELETE(tableName: finalDataSourceName, idFieldName: IDField.TableField.ColumnName, ID: ID);

                whereConditions.Add(IDField.TableField.ColumnName, Convert.ChangeType(dataObjectAttrValue, IDField.TableField.FieldType));
                return DBRoutines.DELETE(tableName: finalDataSourceName, wherePart: whereConditions);

            }//end-inner-if-else
        }


        public virtual T GetById(long id, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
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
            if (Schema.DataSourceType == GLOBALS.DataSource.Type.DBTable)
            {
                //switch (IncludeDataRelations)
                //{
                //    case true:
                //        //Get our data relations list (SqlJoinRelation objects)
                //        dataRelations = GetDataRelations();
                //        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, condition, dataRelations, maximumLimit);
                //        break;

                //    case false:
                //        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, condition, maximumLimit);
                //        break;
                //}

                dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, condition, maximumLimit);
            }

            //It will either return a data table with one row or zero rows
            if (dt.Rows.Count == 0)
            {
                return (T)Activator.CreateInstance(typeof(T));
            }
            else
            {
                return dt.ConvertToList<T>().FirstOrDefault<T>() ?? null;
            }
        }


        public virtual IEnumerable<T> Get(Expression<Func<T, bool>> predicate, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
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

            return dt.ConvertToList<T>();
        }


        public virtual IEnumerable<T> Get(Dictionary<string, object> whereConditions, int limit = 25, string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
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
            if (Schema.DataSourceType == GLOBALS.DataSource.Type.DBTable)
            {
                //switch (IncludeDataRelations)
                //{
                //    case true:
                //        //Get our data relations list (SqlJoinRelation objects)
                //        dataRelations = GetDataRelations();
                //        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, whereConditions, dataRelations, 0);
                //        break;

                //    case false:
                //        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, limit);
                //        break;
                //}

                dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, limit);
            }

            return dt.ConvertToList<T>();
        }


        public virtual IEnumerable<T> GetAll(string dataSourceName = null, GLOBALS.DataSource.Type dataSourceType = GLOBALS.DataSource.Type.Default)
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
            if (Schema.DataSourceType == GLOBALS.DataSource.Type.DBTable)
            {
                //switch(IncludeDataRelations)
                //{ 
                //    case true:
                //        //Get our data relations list (SqlJoinRelation objects)
                //        dataRelations = GetDataRelations();
                //        dt = DBRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, null, dataRelations, 0);
                //        break;

                //    case false:
                //        dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, maximumLimit);
                //        break;
                //}

                dt = DBRoutines.SELECT(finalDataSourceName, thisModelTableColumns, whereConditions, maximumLimit);
            }

            return dt.ConvertToList<T>();
        }


        public virtual IEnumerable<T> GetAll(string SQL_QUERY)
        {
            DataTable dt = DBRoutines.SELECTFROMSQL(SQL_QUERY);

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
