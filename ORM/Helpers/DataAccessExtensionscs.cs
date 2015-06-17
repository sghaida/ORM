using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using ORM.DataAccess;

namespace ORM.Helpers
{
    public static class DataAccessExtensionscs
    {
        private static readonly DbLib DbRoutines = new DbLib();

        public static T GetWithRelations<T>(this T source, params Expression<Func<T, object>>[] path)
            where T : DataModel, new()
        {
            var schema = new DataSourceSchema<T>();

            // Table Relations Map
            // To be sent to the DB Lib for SQL Query generation
            var tableRelationsMap = new List<SqlJoinRelation>();

            //
            // Database related
            // Where conditions dictionary
            //var finalDataSourceName = string.Empty;
            var whereConditions = new Dictionary<string, object>();


            // This will hold the information about the sub joins object types          
            var expressionLookup = path.ToDictionary(t =>
            {
                var memberExpression = t.Body as MemberExpression;
                return memberExpression != null ? memberExpression.Member.Name : null;
            }, t => t.Body.Type.Name);


            //
            // Get the Relations Fields from the Schema 
            var dbRelationsList = schema.DataFields
                .Where(field =>
                    field.Relation != null &&
                    expressionLookup.Values.Contains(field.Relation.WithDataModel.Name) &&
                    expressionLookup.Keys.Contains(field.Name))
                .Select(field => field.Relation).
                ToList();


            //
            // Start processing the list of table relations
            if (dbRelationsList.Any())
            {
                //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                foreach (var relation in dbRelationsList)
                {
                    //Create a temporary map for this target table relation
                    var joinedTableInfo = new SqlJoinRelation();

                    //Get the data model we're in relation with.
                    var relationType = relation.WithDataModel;

                    //Build a data source schema for the data model we're in relation with.
                    var generalModelSchemaType = typeof(DataSourceSchema<>);
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
                        schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                    if (thisKey != null && joinedModelKey != null)
                    {
                        //Initialize the temporary map and add it to the original relations map
                        joinedTableInfo.RelationName = relation.RelationName;
                        joinedTableInfo.RelationType = relation.RelationType;
                        joinedTableInfo.MasterTableName = schema.DataSourceName;
                        joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                        joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                        //Add the relation keys to the TableRelationsMap
                        tableRelationsMap.Add(joinedTableInfo);
                    }
                } //end-foreach
            } //end-outer-if


            //
            // Get the ID Field to find the relations for.
            // If the ID Field was not found, return an empty instance of the object.
            var idField = schema.DataFields.Find(field => field.TableField != null && field.TableField.IsIdField);

            if (idField != null)
            {
                var dataObjectAttr = source.GetType().GetProperty(idField.Name);

                var dataObjectAttrValue = dataObjectAttr.GetValue(source, null);

                // Put the ID Field in the WHERE CONDITIONS
                if (dataObjectAttrValue != null)
                {
                    whereConditions.Add(idField.TableField.ColumnName,
                        Convert.ChangeType(dataObjectAttrValue, idField.TableField.FieldType));
                }
                else
                {
                    return source;
                }
            }
            else
            {
                return source;
            }

            //
            // Get our table columns from the schema
            var thisModelTableColumns = schema.DataFields
                .Where(field => field.TableField != null)
                .Select(
                    field => field.TableField.ColumnName)
                .ToList();

            //
            // Query the data-srouce
            var dt = DbRoutines.SELECT_WITH_JOIN(schema.DataSourceName, thisModelTableColumns, whereConditions,
                tableRelationsMap, 1);

            // Return data
            var data = dt.ConvertToList(path);

            if (data != null && data.Count > 0)
            {
                return data.First();
            }
            return source;
        }

        /// <summary>
        ///     This extension Method works for IEnumerable Types, it exactly select from the Database with first level Join
        /// </summary>
        /// <typeparam name="T">Type</typeparam>
        /// <param name="source">IEnumerable data which to be evaluated and filled by the extension method</param>
        /// <param name="dataSourceName">The DataSource that you wish to select from incase of Distributed Datasources</param>
        /// <param name="path">the path of the relation such as item=>item.x</param>
        /// <returns>IEnumerable with populated relation </returns>
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public static IEnumerable<T> GetWithRelations<T>(this IEnumerable<T> source, string dataSourceName,
            params Expression<Func<T, object>>[] path) where T : DataModel, new()
        {
            var schema = new DataSourceSchema<T>();

            //Table Relations Map
            //To be sent to the DB Lib for SQL Query generation
            var tableRelationsMap = new List<SqlJoinRelation>();

            //This will hold the information about the sub joins object types          
            var expressionLookup = path.ToDictionary(t =>
            {
                var memberExpression = t.Body as MemberExpression;
                return memberExpression != null ? memberExpression.Member.Name : null;
            }, t => t.Body.Type.Name);

            var dbRelationsList = schema.DataFields
                .Where(field =>
                    field.Relation != null &&
                    expressionLookup.Values.Contains(field.Relation.WithDataModel.Name) &&
                    expressionLookup.Keys.Contains(field.Name))
                .Select(field => field.Relation).
                ToList();


            //Start processing the list of table relations
            if (dbRelationsList.Any())
            {
                //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                foreach (var relation in dbRelationsList)
                {
                    //Create a temporary map for this target table relation
                    var joinedTableInfo = new SqlJoinRelation();

                    //Get the data model we're in relation with.
                    var relationType = relation.WithDataModel;

                    //Build a data source schema for the data model we're in relation with.
                    var generalModelSchemaType = typeof(DataSourceSchema<>);
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
                        schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                    if (thisKey != null && joinedModelKey != null)
                    {
                        //Initialize the temporary map and add it to the original relations map
                        joinedTableInfo.RelationName = relation.RelationName;
                        joinedTableInfo.RelationType = relation.RelationType;
                        joinedTableInfo.MasterTableName = dataSourceName ?? schema.DataSourceName;
                        joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                        joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                        //Add the relation keys to the TableRelationsMap
                        tableRelationsMap.Add(joinedTableInfo);
                    }
                } //end-foreach
            } //end-outer-if

            var finalDataSourceName = dataSourceName ?? schema.DataSourceName;


            //Get our table columns from the schema
            var thisModelTableColumns = schema.DataFields
                .Where(field => field.TableField != null)
                .Select(
                    field => field.TableField.ColumnName)
                .ToList();

            var dt = DbRoutines.SELECT_WITH_JOIN(finalDataSourceName, thisModelTableColumns, null, tableRelationsMap, 0);

            return dt.ConvertToList(path);
        }

        /// <summary>
        ///     This extension Method works for IEnumerable Types, it exactly select from the Database with first level Join
        /// </summary>
        /// <typeparam name="T">Type</typeparam>
        /// <param name="source">IEnumerable data which to be evaluated and filled by the extension method</param>
        /// <param name="path">the path of the relation such as item=>item.x</param>
        /// <returns>IEnumerable with populated relation </returns>
        public static IEnumerable<T> GetWithRelations<T>(this IEnumerable<T> source,
            params Expression<Func<T, object>>[] path) where T : DataModel, new()
        {
            var schema = new DataSourceSchema<T>();

            //Table Relations Map
            //To be sent to the DB Lib for SQL Query generation
            var tableRelationsMap = new List<SqlJoinRelation>();

            //This will hold the information about the sub joins object types          
            var expressionLookup = path.ToDictionary(t =>
            {
                var memberExpression = t.Body as MemberExpression;
                return memberExpression != null ? memberExpression.Member.Name : null;
            }, t => t.Body.Type.Name);

            var dbRelationsList = schema.DataFields
                .Where(field =>
                    field.Relation != null &&
                    expressionLookup.Values.Contains(field.Relation.WithDataModel.Name) &&
                    expressionLookup.Keys.Contains(field.Name))
                .Select(field => field.Relation).
                ToList();


            //Start processing the list of table relations
            if (dbRelationsList.Any())
            {
                //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                foreach (var relation in dbRelationsList)
                {
                    //Create a temporary map for this target table relation
                    var joinedTableInfo = new SqlJoinRelation();

                    //Get the data model we're in relation with.
                    var relationType = relation.WithDataModel;

                    //Build a data source schema for the data model we're in relation with.
                    var generalModelSchemaType = typeof(DataSourceSchema<>);
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
                        schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                    if (thisKey == null || joinedModelKey == null) continue;

                    //Initialize the temporary map and add it to the original relations map
                    joinedTableInfo.RelationName = relation.RelationName;
                    joinedTableInfo.RelationType = relation.RelationType;
                    joinedTableInfo.MasterTableName = schema.DataSourceName;
                    joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                    joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                    joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                    joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                    //Add the relation keys to the TableRelationsMap
                    tableRelationsMap.Add(joinedTableInfo);
                } //end-foreach
            } //end-outer-if

            //Get our table columns from the schema
            var thisModelTableColumns = schema.DataFields
                .Where(field => field.TableField != null)
                .Select(
                    field => field.TableField.ColumnName)
                .ToList();

            var dt = DbRoutines.SELECT_WITH_JOIN(schema.DataSourceName, thisModelTableColumns, null, tableRelationsMap, 0);

            return dt.ConvertToList(path);
        }

        [Obsolete("This Function is deprecated. kindly use GetWithRelations instead")]
        public static IEnumerable<T> IncludeRelation<T>(this IEnumerable<T> source, string dataSourceName,
            params Expression<Func<T, object>>[] path) where T : DataModel, new()
        {
            var schema = new DataSourceSchema<T>();
            //Table Relations Map
            //To be sent to the DB Lib for SQL Query generation
            var tableRelationsMap = new List<SqlJoinRelation>();

            //This will hold the information about the sub joins object types          
            var expressionLookup = new Dictionary<string, string>();

            foreach (var t in path)
            {
                var memberExpression = t.Body as MemberExpression;
                if (memberExpression != null)
                    expressionLookup.Add(memberExpression.Member.Name, t.Body.Type.Name);
            }

            var dbRelationsList = schema.DataFields.Where(field => field.Relation != null &&
                                                                    expressionLookup.Values.Contains(
                                                                        field.Relation.WithDataModel.Name) &&
                                                                    expressionLookup.Keys.Contains(field.Name)
                ).
                Select(field => field.Relation).
                ToList();


            //Start processing the list of table relations
            if (dbRelationsList.Any())
            {
                //Foreach relation in the relations list, process it and construct the big TablesRelationsMap
                foreach (var relation in dbRelationsList)
                {
                    //Create a temporary map for this target table relation
                    var joinedTableInfo = new SqlJoinRelation();

                    //Get the data model we're in relation with.
                    var relationType = relation.WithDataModel;

                    //Build a data source schema for the data model we're in relation with.
                    var generalModelSchemaType = typeof(DataSourceSchema<>);
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
                        schema.DataFields.Find(item => item.TableField != null && item.Name == relation.ThisKey);

                    if (thisKey != null && joinedModelKey != null)
                    {
                        //Initialize the temporary map and add it to the original relations map
                        joinedTableInfo.RelationName = relation.RelationName;
                        joinedTableInfo.RelationType = relation.RelationType;


                        joinedTableInfo.MasterTableName = dataSourceName;
                        joinedTableInfo.MasterTableKey = thisKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableName = joinedModelSchema.GetDataSourceName();
                        joinedTableInfo.JoinedTableKey = joinedModelKey.TableField.ColumnName;
                        joinedTableInfo.JoinedTableColumns = joinedModelTableColumns;

                        //Add the relation keys to the TableRelationsMap
                        tableRelationsMap.Add(joinedTableInfo);
                    }
                } //end-foreach
            } //end-outer-if

            //Get our table columns from the schema
            var thisModelTableColumns = schema.DataFields
                .Where(field => field.TableField != null)
                .Select(
                    field => field.TableField.ColumnName)
                .ToList();

            var dt = DbRoutines.SELECT_WITH_JOIN(dataSourceName, thisModelTableColumns, null, tableRelationsMap, 0);

            return dt.ConvertToList(path);
        }
    }
}