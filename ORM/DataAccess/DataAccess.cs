using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using ORM.Exceptions;
using ORM.Helpers;

namespace ORM.DataAccess
{
    public class DataAccess<T> : IDataAccess<T> where T : DataModel, new()
    {
        private static readonly DbLib DbRoutines = new DbLib();

        private static readonly List<Type> NumericTypes = new List<Type>
        {
            typeof (int),
            typeof (long),
            typeof (Int16),
            typeof (Int32),
            typeof (Int64)
        };

        /**
         * Private instance variables
         */
        private readonly DataSourceSchema<T> _schema;
        /**
         * Repository Constructor
         */

        public DataAccess()
        {
            //Get the Table Name and List of Class Attributes
            //Initialize the schema for the class T
            _schema = new DataSourceSchema<T>();

            //Check for absent or invalid DataModel attributes and throw the respective exception if they exist.
            if (string.IsNullOrEmpty(_schema.DataSourceName))
            {
                throw new NoDataSourceNameException(typeof(T).Name);
            }
            if (!_schema.DataFields.Where(item => item.TableField != null).ToList().Any())
            {
                throw new NoTableFieldsException(typeof(T).Name);
            }
            if (string.IsNullOrEmpty(_schema.IdFieldName))
            {
                throw new NoTableFieldsException(typeof(T).Name);
            }
        }

        public virtual int Insert(T dataObject, string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            var rowId = 0;
            string finalDataSourceName;
            var columnsValues = new Dictionary<string, object>();


            //
            // Decide the DataSource Name
            if (false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if (false == string.IsNullOrEmpty(_schema.DataSourceName))
            {
                finalDataSourceName = _schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name +
                                    ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Process the data object and attempt to insert it into the data source
            if (dataObject != null)
            {
                // Get only the Data Fields from Schema which have TableFields objects
                var objectSchemaFields = _schema.DataFields
                    .Where(field => field.TableField != null)
                    .ToList();

                foreach (var field in objectSchemaFields)
                {
                    // Don't insert the ID Field in the Data Source, unless it's marked as AllowIDInsert
                    var skipIdInsert = (field.TableField.IsIdField && field.TableField.AllowIdInsert == false);
                    var skipExcludedColumn = field.TableField.ExcludeOnInsert;

                    if (skipIdInsert || skipExcludedColumn)
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
                            if (NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey)
                                {
                                    //continue;
                                    throw new Exception("The Property " + field.TableField.ColumnName + " in the " +
                                                        dataObject.GetType().Name +
                                                        " Table is a foreign key and it is not allowed to be null. Kindly set the property value.");
                                }
                            }

                            columnsValues.Add(field.TableField.ColumnName,
                                Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                        else
                        {
                            throw new Exception("The Property " + field.TableField.ColumnName + " in the " +
                                                dataObject.GetType().Name +
                                                " Table is not allowed to be null kindly annotate the property with [IsAllowNull]");
                        }
                    }
                    else
                    {
                        if (dataObjectAttr != null)
                        {
                            var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                            if (dataObjectAttrValue != null)
                            {
                                //
                                // Only update the int/long values to zeros if they are not foreign keys
                                if (NumericTypes.Contains(field.TableField.FieldType))
                                {
                                    var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                    if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey)
                                    {
                                        continue;
                                    }
                                }

                                columnsValues.Add(field.TableField.ColumnName,
                                    Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                            }
                        }
                    }
                    //end-inner-if
                } //end-foreach

                rowId = DbRoutines.Insert(finalDataSourceName, columnsValues, _schema.IdFieldName);
            } //end-outer-if

            return rowId;
        }

        public virtual bool Update(T dataObject, string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            var status = false;
            string finalDataSourceName;
            var columnsValues = new Dictionary<string, object>();
            var whereConditions = new Dictionary<string, object>();

            //
            // Decide the DataSource Name 
            if (false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if (false == string.IsNullOrEmpty(_schema.DataSourceName))
            {
                finalDataSourceName = _schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name +
                                    ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Process the data object and attempt to insert it into the data source
            if (dataObject != null)
            {
                // Get only the Data Fields from Schema which have TableFields objects
                var objectSchemaFields = _schema.DataFields
                    .Where(field => field.TableField != null)
                    .ToList();

                foreach (var field in objectSchemaFields)
                {
                    // Get the property value
                    var dataObjectAttr = dataObject.GetType().GetProperty(field.Name);

                    //
                    // Don't update the ID Field in the Data Source, unless it's marked as AllowIDInsert
                    // Add the data object ID into the WHERE CONDITIONS
                    if (field.TableField.IsIdField)
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
                                whereConditions.Add(field.TableField.ColumnName,
                                    Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                            }
                        }
                        else
                        {
                            throw new Exception("The Property " + field.TableField.ColumnName + " in the " +
                                                dataObject.GetType().Name +
                                                " Table is not SET! Kindly please set it to it's original value in order to decide what data to update accordingly.");
                        }


                        //
                        // DON'T CONTINUE EXECUTION IF THE ID FIELD IS NOT ALLOWED TO BE CHANGED
                        if (false == field.TableField.AllowIdInsert)
                        {
                            continue;
                        }
                    }


                    //
                    // Skip the column if it was marked as ExcludeOnUpdate
                    if (field.TableField.ExcludeOnUpdate)
                    {
                        continue;
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
                            if (NumericTypes.Contains(field.TableField.FieldType))
                            {
                                var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey)
                                {
                                    //continue;
                                    throw new Exception("The Property " + field.TableField.ColumnName + " in the " +
                                                        dataObject.GetType().Name +
                                                        " Table is a foreign key and it is not allowed to be null. Kindly set the property value.");
                                }
                            }

                            columnsValues.Add(field.TableField.ColumnName,
                                Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                        }
                    }
                    else
                    {
                        if (dataObjectAttr != null)
                        {
                            var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

                            if (dataObjectAttrValue != null)
                            {
                                //
                                // Only update the int/long values to zeros if they are not foreign keys
                                if (NumericTypes.Contains(field.TableField.FieldType))
                                {
                                    var value = Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType);

                                    if (Convert.ToInt64(value) <= 0 && field.TableField.IsKey)
                                    {
                                        continue;
                                    }
                                }

                                columnsValues.Add(field.TableField.ColumnName,
                                    Convert.ChangeType(dataObjectAttrValue, field.TableField.FieldType));
                            }
                        }
                    }
                    //end-inner-if
                } //end-foreach

                try
                {
                    if (0 == whereConditions.Count)
                    {
                        throw new Exception(
                            "Update Error: Cannot update data object unless there is at least one WHERE CONDITION. Please revise the update procedures on " +
                            dataObject.GetType().Name);
                    }
                    status = DbRoutines.Update(finalDataSourceName, columnsValues, whereConditions);
                }
                catch (Exception ex)
                {
                    throw ex.InnerException;
                }
            } //end-outer-if

            return status;
        }

        public virtual bool Delete(T dataObject, string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            string finalDataSourceName;
            var whereConditions = new Dictionary<string, object>();

            DataField idField;
            //var objectFieldNameWithIdAttribute = string.Empty;


            //
            // Decide the DataSource Name 
            if (false == string.IsNullOrEmpty(dataSourceName))
            {
                finalDataSourceName = dataSourceName;
            }
            else if (false == string.IsNullOrEmpty(_schema.DataSourceName))
            {
                finalDataSourceName = _schema.DataSourceName;
            }
            else
            {
                throw new Exception("Insert Error: No Data Source was provided in the " + dataObject.GetType().Name +
                                    ". Kindly review the class definition or the data mapper definition.");
            }


            //
            // Decide the IDField value
            idField = _schema.DataFields.Find(field => field.TableField != null && field.TableField.IsIdField);

            if (null == idField)
            {
                throw new Exception(
                    "Delete Error: The Data Model does not have IDField property. Kindly mark the properties of " +
                    typeof(T).Name + " with [IsIDField].");
            }


            //
            // Get the object field that is marked with the IsIDField attribute
            var dataObjectAttr = dataObject.GetType().GetProperty(idField.Name);

            var dataObjectAttrValue = dataObjectAttr.GetValue(dataObject, null);

            if (dataObjectAttrValue == null)
            {
                throw new Exception(
                    "The ID Field's value is to NULL. Kindly set the value of the ID Field for the object of type: " +
                    typeof(T).Name);
            }
            //long.TryParse(dataObjectAttrValue.ToString(), out ID);
            //return DBRoutines.DELETE(tableName: finalDataSourceName, idFieldName: IDField.TableField.ColumnName, ID: ID);

            whereConditions.Add(idField.TableField.ColumnName,
                Convert.ChangeType(dataObjectAttrValue, idField.TableField.FieldType));
            return DbRoutines.Delete(finalDataSourceName, whereConditions);
        }

        public virtual T GetById(long id, string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            var dt = new DataTable();

            const int maximumLimit = 1;

            //Get our table columns from the schema
            var thisModelTableColumns = _schema.DataFields
                .Where(field => field.TableField != null)
                .Select(field => field.TableField.ColumnName)
                .ToList();

            //Decide on the Data Source Name
            var finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? _schema.DataSourceName : dataSourceName);

            //Validate the presence of the ID
            if (id <= 0)
            {
                var errorMessage = String.Format("The ID Field is either null or zero. Kindly pass a valid ID. Class name: \"{0}\".",
                    typeof(T).Name);
                throw new Exception(errorMessage);
            }

            //Construct the record ID condition
            var condition = new Dictionary<string, object>();
            condition.Add(_schema.IdFieldName, id);

            //Proceed with getting the data
            if (_schema.DataSourceType == Globals.DataSource.Type.DbTable)
            {
                dt = DbRoutines.Select(finalDataSourceName, thisModelTableColumns, condition, maximumLimit);
            }

            //It will either return a data table with one row or zero rows
            if (dt.Rows.Count == 0)
            {
                return (T)Activator.CreateInstance(typeof(T));
            }
            return dt.ConvertToList<T>().FirstOrDefault<T>();
        }

        public virtual IEnumerable<T> Get(Expression<Func<T, bool>> predicate, string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            DataTable dt;

            if (predicate == null)
            {
                var errorMessage = string.Format("There is no defined Predicate. {0} ", typeof(T).Name);

                throw new Exception(errorMessage);
            }
            var ev = new CustomExpressionVisitor();

            var whereClause = ev.Translate(predicate);

            if (string.IsNullOrEmpty(dataSourceName))
            {
                if (string.IsNullOrEmpty(whereClause))
                {
                    dt = DbRoutines.Select(_schema.DataSourceName);
                }
                else
                {
                    dt = DbRoutines.Select(_schema.DataSourceName, whereClause);
                }
            }
            else
            {
                if (string.IsNullOrEmpty(whereClause))
                {
                    dt = DbRoutines.Select(dataSourceName);
                }
                else
                {
                    dt = DbRoutines.Select(dataSourceName, whereClause);
                }
            }

            return dt.ConvertToList<T>();
        }

        public virtual IEnumerable<T> Get(Dictionary<string, object> whereConditions, int limit = 25,
            string dataSourceName = null, Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            var dt = new DataTable();

            //Get our table columns from the schema
            var thisModelTableColumns = _schema.DataFields
                .Where(field => field.TableField != null)
                .Select(field => field.TableField.ColumnName)
                .ToList();

            //Decide on the Data Source Name
            var finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? _schema.DataSourceName : dataSourceName);

            //Validate the presence of the where conditions
            if (whereConditions == null || whereConditions.Count == 0)
            {
                var errorMessage = String.Format(
                    "The \"whereConditions\" parameter is either null or empty. Kindly pass a valid \"whereConditions\" parameter. Class name: \"{0}\".",
                    typeof(T).Name);
                throw new Exception(errorMessage);
            }


            //Proceed with getting the data
            if (_schema.DataSourceType == Globals.DataSource.Type.DbTable)
            {
                dt = DbRoutines.Select(finalDataSourceName, thisModelTableColumns, whereConditions, limit);
            }

            return dt.ConvertToList<T>();
        }

        public virtual IEnumerable<T> GetAll(string dataSourceName = null,
            Globals.DataSource.Type dataSourceType = Globals.DataSource.Type.Default)
        {
            var dt = new DataTable();

            const int maximumLimit = 0;

            //Get our table columns from the schema
            var thisModelTableColumns = _schema.DataFields
                .Where(field => field.TableField != null)
                .Select(field => field.TableField.ColumnName)
                .ToList();

            //Decide on the Data Source Name
            var finalDataSourceName = (string.IsNullOrEmpty(dataSourceName) ? _schema.DataSourceName : dataSourceName);

            //Proceed with getting the data
            if (_schema.DataSourceType == Globals.DataSource.Type.DbTable)
            {
                dt = DbRoutines.Select(finalDataSourceName, thisModelTableColumns, null, maximumLimit);
            }

            return dt.ConvertToList<T>();
        }

        public virtual IEnumerable<T> GetAll(string sqlQuery)
        {
            var dt = DbRoutines.Selectfromsql(sqlQuery);

            return dt.ConvertToList<T>();
        }

        public virtual int Insert(string sql)
        {
            var id = DbRoutines.Insert(sql);

            return id;
        }

        public virtual bool Update(string sql)
        {
            var status = DbRoutines.Update(sql);

            return status;
        }

        public virtual bool Delete(string sql)
        {
            var status = DbRoutines.Delete(sql);

            return status;
        }

        /*
                /// <summary>
                ///     This is a private function. It is responsible for returning a list of the data relations on this data model
                ///     translated to a list of SqlJoinRelation objects.
                /// </summary>
                /// <returns>List of SqlJoinRelation objects</returns>
                private List<SqlJoinRelation> GetDataRelations()
                {
                    //Table Relations Map
                    //To be sent to the DB Lib for SQL Query generation
                    var tableRelationsMap = new List<SqlJoinRelation>();

                    //TableRelationsList
                    //To be used to looking up the relations and extracting information from them and copying them into the TableRelationsMap
                    var dbRelationsList =
                        _schema.DataFields.Where(field => field.Relation != null).Select(field => field.Relation).ToList();

                    //Start processing the list of table relations
                    if (dbRelationsList != null && dbRelationsList.Count() > 0)
                    {
                        //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                        foreach (var relation in dbRelationsList)
                        {
                            //Create a temporary map for this target table relation
                            var joinedTableInfo = new SqlJoinRelation();

                            //Get the data model we're in relation with.
                            var relationType = relation.WithDataModel;

                            //Build a data source schema for the data model we're in relation with.
                            var generalModelSchemaType = typeof (DataSourceSchema<>);
                            var specialModelSchemaType = generalModelSchemaType.MakeGenericType(relationType);
                            dynamic joinedModelSchema = Activator.CreateInstance(specialModelSchemaType);

                            //Get it's Data Fields.
                            List<DataField> joinedModelFields = joinedModelSchema.GetDataFields();

                            //Get the table column names - exclude the ID field name.
                            var joinedModelTableColumns = joinedModelFields
                                .Where(field => field.TableField != null)
                                .Select(field => field.TableField.ColumnName)
                                .ToList();

                            //Get the field that describes the relation key from the target model schema
                            var joinedModelKey =
                                joinedModelFields.Find(item => item.TableField != null && item.Name == relation.OnDataModelKey);

                            //Get the field that describes our key on which we are in relation with the target model
                            var thisKey =
                                _schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                            if (thisKey != null && joinedModelKey != null)
                            {
                                //Initialize the temporary map and add it to the original relations map
                                joinedTableInfo.RelationName = relation.RelationName;
                                joinedTableInfo.RelationType = relation.RelationType;
                                joinedTableInfo.MasterTableName = _schema.DataSourceName;
                                joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                                joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                                joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                                joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                                //Add the relation keys to the TableRelationsMap
                                tableRelationsMap.Add(joinedTableInfo);
                            }
                        } //end-foreach
                    } //end-outer-if

                    return tableRelationsMap;
                }
        */
    }
}