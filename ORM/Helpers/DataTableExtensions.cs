using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using ORM.DataAccess;
using ORM.DataAttributes;

namespace ORM.Helpers
{
    public static class DataTableExtentions
    {
        // 
        // Helper function
        private static string ConvertToDateString(object date)
        {
            return date == null ? string.Empty : Convert.ToDateTime(date).ConvertDate();
        } //end-ConvertToDateString-function

        [Obsolete]
        public static List<T> ConvertToList_OLD<T>(this DataTable dataTable) where T : class, new()
        {
            var dataList = new List<T>();


            // List of class property infos
            //var masterPropertyInfoFields = new List<PropertyInfo>();
            //var cdtPropertyInfo = new Dictionary<string, List<ObjectPropertyInfoField>>();

            //List of T object data fields (DbColumnAttribute Values), and types.
            var masterObjectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            // Initialize Master the property info fields list
            var masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .ToList();

            //Read Datatable column names and types
            //var dtlFieldNames = dataTable.Columns.Cast<DataColumn>()
            //    .Select(item => new
            //    {
            //        Name = item.ColumnName,
            //        Type = item.DataType
            //    }).ToList();

            // Initialize the object data fields  list for Master Object
            foreach (var item in masterPropertyInfoFields)
            {
                masterObjectFields.Add(new ObjectPropertyInfoField
                {
                    Property = item,
                    DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                    DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                });
            }


            //Fill The data
            //foreach (var datarow in DataTable.AsEnumerable().ToList())   
            //{
            Parallel.ForEach(dataTable.AsEnumerable().ToList(),
                datarow =>
                {
                    var masterObj = new T();

                    //Fill master Object with its related properties values
                    foreach (var dtField in masterObjectFields)
                    {
                        if (dtField != null)
                        {
                            // Get the property info object of this field, for easier accessibility
                            var dataFieldPropertyInfo = dtField.Property;

                            if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnDateTimeMinIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    Convert.ToDecimal(datarow[dtField.DataFieldName].ReturnZeroIfNull()), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                            {
                                if (datarow[dtField.DataFieldName] is DateTime)
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj,
                                        ConvertToDateString(datarow[dtField.DataFieldName]), null);
                                }
                                else
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj,
                                        datarow[dtField.DataFieldName].ReturnEmptyIfNull(), null);
                                }
                            }
                        } //end if
                    } //end foreach

                    lock (dataList)
                    {
                        dataList.Add(masterObj);
                    }
                });
            //}

            return dataList;
        }

        [Obsolete]
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public static List<T> ConvertToList_OLD<T>(this DataTable dataTable, params Expression<Func<T, object>>[] path)
            where T : class, new()
        {
            var dataList = new List<T>();

            // List of class property infos
            //var masterPropertyInfoFields = new List<PropertyInfo>();
            var childPropertInfoFields = new List<PropertyInfo>();

            var childrenObjectsProperties = new Dictionary<string, List<ObjectPropertyInfoField>>();
            var cdtPropertyInfo = new Dictionary<string, List<ObjectPropertyInfoField>>();

            //List of T object data fields (DbColumnAttribute Values), and types.
            var masterObjectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            var expressionLookup = new Dictionary<string, string>();

            foreach (var t in path)
            {
                var memberExpression = t.Body as MemberExpression;
                if (memberExpression != null)
                    expressionLookup.Add(memberExpression.Member.Name, t.Body.Type.Name);
            }

            // Initialize Master the property info fields list
            var masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                 .ToList();

            //Read Datatable column names and types
            var dtlFieldNames = dataTable.Columns.Cast<DataColumn>()
                .Select(item => new
                {
                    Name = item.ColumnName,
                    Type = item.DataType
                }).ToList();

            // Initialize the object data fields  list for Master Object
            foreach (var item in masterPropertyInfoFields)
            {
                masterObjectFields.Add(new ObjectPropertyInfoField
                {
                    Property = item,
                    DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                    DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                });
            }


            if (path.Any())
            {
                // Initialize child the property info fields list
                childPropertInfoFields = typeof(T).GetProperties(flags)
                    .Where(
                        property =>
                            property.GetCustomAttribute<DataRelationAttribute>() != null &&
                            expressionLookup.Keys.Contains(property.Name))
                    .ToList();

                // Fill the childrenObjectsProperties dictionary with the name of the children class for reflection and their corrospndant attributes
                foreach (var property in childPropertInfoFields)
                {
                    var childtypedObject = property.PropertyType;

                    var childtableFields = childtypedObject.GetProperties(flags)
                        .Where(item => item.GetCustomAttribute<DbColumnAttribute>() != null)
                        .Select(item => new ObjectPropertyInfoField
                        {
                            Property = item,
                            DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                            DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                        })
                        .ToList();

                    var tableName = property.GetCustomAttribute<DataRelationAttribute>().Name;
                    childrenObjectsProperties.Add(tableName, childtableFields);
                }

                //Get the Children classes related columns from datatable
                foreach (var childObjectsProperties in childrenObjectsProperties)
                {
                    var childObjectColumns = (from childObjField in childObjectsProperties.Value
                                              join dtlFieldName in dtlFieldNames on
                                                  (childObjectsProperties.Key + "." + childObjField.DataFieldName) equals dtlFieldName.Name
                                              where dtlFieldName.Type == childObjField.DataFieldType
                                              select
                                                  new ObjectPropertyInfoField
                                                  {
                                                      DataFieldName = dtlFieldName.Name,
                                                      DataFieldType = dtlFieldName.Type,
                                                      Property = childObjField.Property,
                                                      ObjectFieldName = childObjField.DataFieldName
                                                  }).ToList();

                    if (childObjectColumns.Count > 0)
                    {
                        cdtPropertyInfo.Add(childObjectsProperties.Key, childObjectColumns);
                    }
                }
            }

            //Fill The data
            //foreach (var datarow in DataTable.AsEnumerable().ToList())   
            //{
            Parallel.ForEach(dataTable.AsEnumerable().ToList(),
                datarow =>
                {
                    var masterObj = new T();

                    if (path.Any())
                    {
                        //Fill the Data for children objects
                        foreach (var property in childPropertInfoFields)
                        {
                            List<ObjectPropertyInfoField> data;
                            cdtPropertyInfo.TryGetValue(property.GetCustomAttribute<DataRelationAttribute>().Name,
                                out data);

                            // In order not to instantiate unwanted objects
                            if (data != null)
                            {
                                var childtypedObject = property.PropertyType;
                                var childObj = Activator.CreateInstance(childtypedObject);

                                foreach (var dtField in data)
                                {
                                    var dataField = data.Find(item => item.DataFieldName == dtField.DataFieldName);

                                    if (dataField != null)
                                    {
                                        var dataFieldPropertyInfo = dataField.Property;

                                        if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj,
                                                datarow[dtField.DataFieldName].ReturnDateTimeMinIfNull(), null);
                                        }
                                        else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj,
                                                datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                                        }
                                        else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj,
                                                datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                                        }
                                        else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj,
                                                Convert.ToDecimal(datarow[dtField.DataFieldName].ReturnZeroIfNull()),
                                                null);
                                        }
                                        else if (dataFieldPropertyInfo.PropertyType == typeof(Char))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj,
                                                Convert.ToString(datarow[dtField.DataFieldName].ReturnEmptyIfNull()),
                                                null);
                                        }
                                        else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                                        {
                                            if (datarow[dtField.DataFieldName] is DateTime)
                                            {
                                                dataFieldPropertyInfo.SetValue(childObj,
                                                    ConvertToDateString(datarow[dtField.DataFieldName]), null);
                                            }
                                            else
                                            {
                                                dataFieldPropertyInfo.SetValue(childObj,
                                                    datarow[dtField.DataFieldName].ReturnEmptyIfNull(), null);
                                            }
                                        }
                                    }
                                }


                                //Set the values for the children object
                                foreach (var masterPropertyInfo in childPropertInfoFields)
                                {
                                    if (masterPropertyInfo.PropertyType.Name == childObj.GetType().Name)
                                    {
                                        masterPropertyInfo.SetValue(masterObj, childObj);
                                    }
                                }
                            }
                        } // end foreach
                    }
                    //Fill master Object with its related properties values
                    foreach (var dtField in masterObjectFields)
                    {
                        if (dtField != null)
                        {
                            // Get the property info object of this field, for easier accessibility
                            var dataFieldPropertyInfo = dtField.Property;

                            if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnDateTimeMinIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj,
                                    Convert.ToDecimal(datarow[dtField.DataFieldName].ReturnZeroIfNull()), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                            {
                                if (datarow[dtField.DataFieldName] is DateTime)
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj,
                                        ConvertToDateString(datarow[dtField.DataFieldName]), null);
                                }
                                else
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj,
                                        datarow[dtField.DataFieldName].ReturnEmptyIfNull(), null);
                                }
                            }
                        } //end if
                    } //end foreach

                    lock (dataList)
                    {
                        dataList.Add(masterObj);
                    }
                });
            //}

            return dataList;
        }

        public static List<T> ConvertToList<T>(this DataTable dataTable) where T : class, new()
        {
            var dataList = new List<T>();

            var setters = new Dictionary<string, Action<T, object>>();


            //List of T object data fields (DbColumnAttribute Values), and types.
            //var masterObjectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            var masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .ToList();

            foreach (var field in masterPropertyInfoFields)
            {
                var propertyInfo = typeof(T).GetProperty(field.Name);
                var columnName = field.GetCustomAttribute<DbColumnAttribute>().Name;
                setters.Add(columnName, Invoker.CreateSetter<T>(propertyInfo));
            }

            Parallel.ForEach(dataTable.AsEnumerable().ToList(),
                datarow =>
                {
                    var masterObj = new T();

                    foreach (var setter in setters)
                    {
                        if (!datarow.Table.Columns.Contains(setter.Key) || datarow[setter.Key] == null ||
                            datarow[setter.Key] == DBNull.Value)
                        {
                        }
                        else
                        {
                            setter.Value(masterObj, datarow[setter.Key]);
                        }
                    }

                    lock (dataList)
                    {
                        dataList.Add(masterObj);
                    }
                }
                );

            return dataList;
        }

        /// <summary>
        ///     Converts datatable to list&lt;T&gt; dynamically
        /// </summary>
        /// <typeparam name="T">Class name</typeparam>
        /// <param name="dataTable">data table to convert</param>
        /// <param name="path"></param>
        /// <returns>List&lt;T&gt;</returns>
        public static List<T> ConvertToList<T>(this DataTable dataTable, params Expression<Func<T, object>>[] path)
            where T : class, new()
        {
            var dataList = new List<T>();

            //
            // List of class property infos
            List<PropertyInfo> childPropertInfoFields;

            //
            // List of T object data fields (DbColumnAttribute Values), and types.
            var settersMasterObject = new Dictionary<string, Action<T, object>>();

            //
            // Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            var expressionLookup = new Dictionary<string, string>();

            foreach (var t in path)
            {
                var memberExpression = t.Body as MemberExpression;

                if (memberExpression != null)
                    expressionLookup.Add(memberExpression.Member.Name, t.Body.Type.Name);
            }


            // Begin
            // Initialize Master the property info fields list
            var masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .ToList();

            // Initialize the master object setters dictionary
            foreach (var field in masterPropertyInfoFields)
            {
                var propertyInfo = typeof(T).GetProperty(field.Name);
                var columnName = field.GetCustomAttribute<DbColumnAttribute>().Name;
                settersMasterObject.Add(columnName, Invoker.CreateSetter<T>(propertyInfo));
            }

            //
            // Fill The data
            //foreach (var datarow in DataTable.AsEnumerable().ToList())
            //{
            Parallel.ForEach(dataTable.AsEnumerable().ToList(), datarow =>
            {
                // Create and instance of the master object type
                var masterObj = new T();

                //
                // Process the path, in case there are relations
                if (path.Any())
                {
                    // Initialize child the property info fields list
                    childPropertInfoFields = typeof(T).GetProperties(flags)
                        .Where(
                            property =>
                                property.GetCustomAttribute<DataRelationAttribute>() != null &&
                                expressionLookup.Keys.Contains(property.Name))
                        .ToList();


                    // Fill the Data for children objects
                    foreach (var property in childPropertInfoFields)
                    {
                        //
                        // Get the type of the child object
                        var typeOfChildObject = property.PropertyType;

                        //
                        // Construct the dictionary of this child setters
                        // something like: Dictionary<FieldColumnName, SetterFunction>
                        // Dictionary Type: Dictionary<string, Action<T, object>>();
                        // First create the setter Action delegate Type: Action<T, object>
                        var reflectionType = typeof(ReflectionHelper<>);
                        var genericReflectionType = reflectionType.MakeGenericType(typeOfChildObject);
                        dynamic childReflection = Activator.CreateInstance(genericReflectionType);
                        dynamic childSetters = childReflection.GetReflectionDictionary();

                        //
                        // Get this child-object's relation name
                        var childRelationName = property.GetCustomAttribute<DataRelationAttribute>().Name;

                        //
                        // Get this child-object's fields
                        var childObjectFields = typeOfChildObject.GetProperties(flags)
                            .Where(item => item.GetCustomAttribute<DbColumnAttribute>() != null)
                            .ToList();

                        //
                        // Foreach field in this child-object, add it's setter function to the childSetters dictionary
                        foreach (var field in childObjectFields)
                        {
                            var propertyInfo = typeOfChildObject.GetProperty(field.Name);
                            var columnName = field.GetCustomAttribute<DbColumnAttribute>().Name;

                            object[] setterMethodParams = { propertyInfo };
                            var buildUntypedSetterMethod = typeof(Invoker).GetMethod("CreateSetter");
                            var genericSetterMethod = buildUntypedSetterMethod.MakeGenericMethod(typeOfChildObject);
                            dynamic setter = genericSetterMethod.Invoke(null, setterMethodParams);

                            // Fill the setter in he childSetters dictionary
                            childSetters.Add(columnName, setter);
                        }

                        //
                        // Make an instance of the child-object type
                        dynamic childObj = Activator.CreateInstance(typeOfChildObject);

                        //
                        // Fill the child object through calling it's own setters
                        foreach (var setter in childSetters)
                        {
                            string columnName = String.Format("{0}.{1}", childRelationName, setter.Key);

                            try
                            {
                                var value = datarow[columnName];
                                setter.Value(childObj, value);
                            }
                            catch (Exception ex)
                            {
                                throw ex.InnerException;
                            }
                        }

                        //
                        // Set the values for the children-object in the master-object
                        foreach (var masterPropertyInfo in childPropertInfoFields)
                        {
                            if (masterPropertyInfo.PropertyType.Name == childObj.GetType().Name)
                            {
                                masterPropertyInfo.SetValue(masterObj, childObj);
                            }
                        }
                    } //end-outer-foreach
                } //end-if


                //
                // Fill master Object with its related properties values
                foreach (var setter in settersMasterObject)
                {
                    setter.Value(masterObj, datarow[setter.Key]);
                }

                lock (dataList)
                {
                    dataList.Add(masterObj);
                }
            });
            //}

            return dataList;
        }

        /// <summary>
        ///     Converts List&lt;T&gt; to Datatable
        /// </summary>
        /// <typeparam name="T">ClassName</typeparam>
        /// <param name="list">List to be converted</param>
        /// <returns>Populated DataTable</returns>
        public static DataTable ConvertToDataTable<T>(this List<T> list) where T : class, new()
        {
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            var getters = new Dictionary<string, Func<T, object>>();

            var dt = new DataTable(typeof(T).Name);

            //Get all the properties
            var masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property =>
                    property.GetCustomAttribute<DbColumnAttribute>() != null &&
                    property.GetCustomAttribute<DbColumnAttribute>().Name != "PhoneCallsTableName")
                .ToList();

            //var propertiesLength = masterPropertyInfoFields.ToArray().Length;

            foreach (var field in masterPropertyInfoFields)
            {
                var propertyInfo = typeof(T).GetProperty(field.Name);
                var columnName = field.GetCustomAttribute<DbColumnAttribute>().Name;

                var getter = Invoker.CreateGetter<T>(propertyInfo);

                getters.Add(columnName, getter);

                var col = new DataColumn(columnName);
                col.DataType = propertyInfo.PropertyType;

                if (col.DataType == typeof(decimal))
                {
                    col.DefaultValue = Convert.ToDecimal(0);
                }
                else if (col.DataType == typeof(string))
                {
                    col.DefaultValue = DBNull.Value;
                }
                else if (col.DataType == typeof(DateTime))
                {
                    col.DefaultValue = SqlDateTime.MinValue.Value;
                    col.DateTimeMode = DataSetDateTime.UnspecifiedLocal;
                }


                //Add Columns 
                dt.Columns.Add(col);
            }

            //only for Compisite Primary Key in our case it is phonecalls
            //dt.PrimaryKey = new[] { dt.Columns[0], dt.Columns[1] };

            //this object will be loacked during parallel loop
            //var status = new object();

            //Add Rows
            Parallel.ForEach(list, phonecall =>
            {
                lock (dt)
                {
                    if (phonecall == null)
                        return;
                    var row = dt.NewRow();
                    foreach (var getter in getters)
                    {
                        //Validate DatetimeMIn and convert it to SQLDateTimeMin
                        if (dt.Columns[getter.Key].DataType == typeof(DateTime) &&
                            (DateTime)getter.Value(phonecall) == DateTime.MinValue)
                            row[getter.Key] = SqlDateTime.MinValue.Value;
                        else
                            row[getter.Key] = getter.Value(phonecall);
                    }

                    dt.Rows.Add(row);
                }
            });

            return dt;
        }

        /// <summary>
        ///     Gets the Name of DB table Field
        /// </summary>
        /// <param name="enumObject"></param>
        /// <returns>Field Description</returns>
        public static string Description(this Enum enumObject)
        {
            var fieldInfo = enumObject.GetType().GetField(enumObject.ToString());

            var descAttributes =
                (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);

            if (descAttributes.Length > 0)
            {
                return descAttributes[0].Description;
            }
            return enumObject.ToString();
        }

        /// <summary>
        ///     Gets the DefaultValue attribute of the enum
        /// </summary>
        /// <param name="enumObject"></param>
        /// <returns>Field Description</returns>
        public static string Value(this Enum enumObject)
        {
            var fieldInfo = enumObject.GetType().GetField(enumObject.ToString());

            var valueAttributes =
                (DefaultValueAttribute[])fieldInfo.GetCustomAttributes(typeof(DefaultValueAttribute), false);

            if (valueAttributes.Length > 0)
            {
                return valueAttributes[0].Value.ToString();
            }
            return enumObject.ToString();
        }

        /// <summary>
        ///     Return an enum object to a list of enums
        /// </summary>
        /// <typeparam name="T">Enum Object</typeparam>
        /// <returns>IEnumerable</returns>
        public static IEnumerable<T> EnumToList<T>()
        {
            var enumType = typeof(T);

            if (enumType.BaseType != typeof(Enum))
            {
                throw new ArgumentException("T is not of System.Enum Type");
            }

            var enumValArray = Enum.GetValues(enumType);
            var enumValList = new List<T>(enumValArray.Length);

            foreach (int val in enumValArray)
            {
                enumValList.Add((T)Enum.Parse(enumType, val.ToString()));
            }

            return enumValList;
        }
    }
}